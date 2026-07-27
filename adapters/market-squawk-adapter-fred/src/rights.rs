use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
};

use market_squawk_domain::{SourceIdentifier, Timestamp};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

/// A checked SHA-256 digest.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Sha256Digest([u8; 32]);

impl Sha256Digest {
    /// Constructs a digest from its exact bytes.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Parses 64 lowercase hexadecimal characters.
    pub fn from_lower_hex(value: &str) -> Result<Self, FredRightsError> {
        if value.len() != 64
            || value
                .bytes()
                .any(|byte| !byte.is_ascii_digit() && !(b'a'..=b'f').contains(&byte))
        {
            return Err(FredRightsError::InvalidDigest);
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
            bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
        }
        Ok(Self(bytes))
    }

    /// Returns the exact digest bytes.
    pub const fn bytes(self) -> [u8; 32] {
        self.0
    }

    fn is_zero(self) -> bool {
        self.0 == [0; 32]
    }

    fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

fn hex_nibble(value: u8) -> Result<u8, FredRightsError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(FredRightsError::InvalidDigest),
    }
}

/// A separately authorized operation over FRED or ALFRED material.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FredOperation {
    /// Transient retrieval without durable retention.
    RetrieveEphemeral,
    /// Durable local persistence.
    Persist,
    /// Local cache retention.
    Cache,
    /// Archival retention.
    Archive,
    /// Redistribution to another party.
    Redistribute,
    /// Display to a user.
    Display,
    /// Model training or fitting.
    Train,
}

/// The fail-closed outcome of a rights assessment.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FredRightsDisposition {
    /// Every requested operation is affirmatively authorized.
    Permitted,
    /// Durable use requires both exact Bank service permission and exact-series authority.
    ServicePermissionRequired,
    /// Verified current terms or an exact-series grant does not establish the requested use.
    BlockedUnknownRights,
    /// Exact-series authority exists, but current Bank service permission is absent or incomplete.
    BlockedServicePermission,
    /// A grant exists but does not include every requested operation.
    BlockedOperationScope,
    /// The terms evidence has crossed its local revalidation deadline.
    BlockedStaleTerms,
}

const TERMS_BUNDLE_DOMAIN: &[u8] = b"market-squawk:fred-terms-bundle:v2\0";
const SERVICE_PERMISSION_DOMAIN: &[u8] = b"market-squawk:fred-service-permission:v1\0";
const DURABLE_AUTHORITY_DOMAIN: &[u8] = b"market-squawk:fred-durable-authority:v1\0";
const SERVICE_PERMISSION_ISSUER: &str = "federal-reserve-bank-of-st-louis";
const SERVICE_PERMISSION_APPLICATION: &str = "market-squawk";
const SERVICE_PERMISSION_SERVICE: &str = "fred-api";
/// Maximum exact bytes accepted for one reviewed FRED terms document.
pub const MAX_FRED_TERMS_DOCUMENT_BYTES: usize = 2 * 1024 * 1024;
/// Maximum exact bytes accepted for one series-rights evidence document.
pub const MAX_FRED_SERIES_RIGHTS_EVIDENCE_BYTES: usize = 256 * 1024;
/// Maximum exact bytes accepted for one St. Louis Fed service-permission document.
pub const MAX_FRED_SERVICE_PERMISSION_BYTES: usize = 256 * 1024;
/// Exact byte length of the code-reviewed FRED rights manifest.
pub const CURRENT_FRED_RIGHTS_ARTIFACT_BYTE_LENGTH: usize = 4_001;
/// SHA-256 identity of the code-reviewed FRED rights manifest.
pub const CURRENT_FRED_RIGHTS_ARTIFACT_SHA256: Sha256Digest = Sha256Digest::from_bytes([
    0xc2, 0xf5, 0x35, 0x99, 0x30, 0x7d, 0x77, 0x39, 0x87, 0x99, 0xcc, 0xda, 0x16, 0xec, 0xa3, 0x4c,
    0x78, 0xdf, 0xa2, 0x16, 0xb9, 0x83, 0xae, 0x95, 0xda, 0xab, 0xae, 0x68, 0x94, 0x98, 0x2e, 0x77,
]);
/// Exact byte length of the reviewed built-in UNRATE public-domain decision.
pub const CURRENT_UNRATE_RIGHTS_ARTIFACT_BYTE_LENGTH: usize = 1_752;
/// SHA-256 identity of the reviewed built-in UNRATE public-domain decision.
pub const CURRENT_UNRATE_RIGHTS_ARTIFACT_SHA256: Sha256Digest = Sha256Digest::from_bytes([
    0xa7, 0x6a, 0xb0, 0x03, 0x4e, 0xba, 0x4b, 0x87, 0x8b, 0x00, 0xa6, 0xec, 0xf2, 0x9f, 0x02, 0x62,
    0x70, 0x63, 0x9d, 0x23, 0x0c, 0x07, 0x4a, 0x29, 0x15, 0x2a, 0x1b, 0x29, 0x89, 0xff, 0x7f, 0x0a,
]);

const CURRENT_TERMS_BUNDLE_SHA256: Sha256Digest = Sha256Digest::from_bytes([
    0x0e, 0xe7, 0x74, 0x3e, 0xe7, 0x52, 0x72, 0x6e, 0xe6, 0x64, 0x64, 0xdb, 0xc2, 0x3b, 0x13, 0xcf,
    0x34, 0xd1, 0xd3, 0x0e, 0x33, 0x46, 0x07, 0xe3, 0x49, 0x02, 0x17, 0xdc, 0x3f, 0xb3, 0xf2, 0x92,
]);
const CURRENT_API_TERMS_SHA256: Sha256Digest = Sha256Digest::from_bytes([
    0xd6, 0x74, 0x87, 0x87, 0xc8, 0xe2, 0xe0, 0xf9, 0xb8, 0x74, 0x6c, 0x21, 0x2b, 0x4d, 0x06, 0xcf,
    0xa9, 0x4d, 0x59, 0x4b, 0x26, 0xbc, 0xe0, 0x56, 0x47, 0xad, 0x10, 0x39, 0x2f, 0x8a, 0x19, 0x07,
]);
const CURRENT_LEGAL_TERMS_SHA256: Sha256Digest = Sha256Digest::from_bytes([
    0x3b, 0x7c, 0x6e, 0x1e, 0x7c, 0xde, 0xd4, 0xa5, 0x5b, 0x3a, 0x24, 0x69, 0xf4, 0x45, 0x94, 0xce,
    0x10, 0xda, 0xd3, 0x0b, 0x30, 0x00, 0x21, 0x75, 0xff, 0xdc, 0xca, 0xa0, 0xf9, 0x45, 0xf4, 0x47,
]);
const CURRENT_PRIVACY_SHA256: Sha256Digest = Sha256Digest::from_bytes([
    0xf0, 0x22, 0xe6, 0x8d, 0x07, 0x75, 0x0c, 0x62, 0x1f, 0x30, 0x9e, 0xaa, 0x87, 0x9f, 0xac, 0x32,
    0x72, 0x9d, 0x09, 0x74, 0xa2, 0xbe, 0x4a, 0x58, 0xbd, 0x5f, 0xbf, 0xe5, 0xc7, 0x25, 0x84, 0x04,
]);
const CURRENT_TERMS_ASSESSED_AT_UNIX_NANOS: i64 = 1_785_024_000_000_000_000;
const CURRENT_TERMS_REVIEW_REQUIRED_BY_UNIX_NANOS: i64 = 1_785_628_800_000_000_000;
const CURRENT_DURABLE_OPERATIONS: [FredOperation; 5] = [
    FredOperation::Display,
    FredOperation::Persist,
    FredOperation::Cache,
    FredOperation::Archive,
    FredOperation::Train,
];
const CURRENT_UNRATE_EVIDENCE_REFERENCE: &str = "https://github.com/Sawmonabo/market-squawk/blob/release/market-squawk-v0.1.0/docs/verification/fred-unrate-public-domain-rights.json";
const CURRENT_UNRATE_AUTHORITY_URL: &str = "https://www.bls.gov/opub/copyright-information.htm";
const CURRENT_UNRATE_CITATION_URL_PREFIX: &str = "https://fred.stlouisfed.org/series/";

/// Required role of one exact document in the FRED terms bundle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub enum FredTermsDocumentRole {
    /// FRED API terms governing API access and incorporated terms.
    ApiTerms,
    /// FRED services and legal terms incorporated by the API terms.
    FredServicesLegalTerms,
    /// Federal Reserve Bank of St. Louis online privacy notice.
    PrivacyPolicy,
}

impl FredTermsDocumentRole {
    const ALL: [Self; 3] = [
        Self::ApiTerms,
        Self::FredServicesLegalTerms,
        Self::PrivacyPolicy,
    ];

    const fn tag(self) -> u8 {
        match self {
            Self::ApiTerms => 1,
            Self::FredServicesLegalTerms => 2,
            Self::PrivacyPolicy => 3,
        }
    }

    /// Returns the sole canonical HTTPS URL for this required document.
    pub const fn canonical_url(self) -> &'static str {
        match self {
            Self::ApiTerms => "https://fred.stlouisfed.org/docs/api/terms_of_use.html",
            Self::FredServicesLegalTerms => "https://fred.stlouisfed.org/legal/",
            Self::PrivacyPolicy => {
                "https://www.stlouisfed.org/about-us/privacy-policy/online-notice"
            }
        }
    }
}

/// Representation used to derive one document's reviewed identity from exact acquired bytes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum FredTermsDocumentRepresentation {
    /// The exact acquired response body is the reviewed representation.
    ExactRaw,
    /// One volatile Sitecore SXA search-component `itemid` is replaced by a fixed token.
    PrivacySxaSearchItemCanonicalV1,
}

impl FredTermsDocumentRepresentation {
    const fn tag(self) -> u8 {
        match self {
            Self::ExactRaw => 1,
            Self::PrivacySxaSearchItemCanonicalV1 => 2,
        }
    }
}

/// Exact caller-supplied bytes for one reviewed FRED terms document.
#[derive(Clone, Copy)]
pub struct FredTermsDocumentBytes<'a> {
    role: FredTermsDocumentRole,
    bytes: &'a [u8],
}

impl<'a> FredTermsDocumentBytes<'a> {
    /// Binds non-empty, bounded exact document bytes to their required role.
    ///
    /// # Errors
    ///
    /// Returns [`FredRightsError::InvalidTermsDocumentBytes`] for empty input or input above
    /// [`MAX_FRED_TERMS_DOCUMENT_BYTES`].
    pub fn try_new(role: FredTermsDocumentRole, bytes: &'a [u8]) -> Result<Self, FredRightsError> {
        if bytes.is_empty() || bytes.len() > MAX_FRED_TERMS_DOCUMENT_BYTES {
            return Err(FredRightsError::InvalidTermsDocumentBytes);
        }
        Ok(Self { role, bytes })
    }

    /// Returns the document role.
    pub const fn role(&self) -> FredTermsDocumentRole {
        self.role
    }

    /// Returns the exact supplied bytes.
    pub const fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
}

impl std::fmt::Debug for FredTermsDocumentBytes<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FredTermsDocumentBytes")
            .field("role", &self.role)
            .field("byte_length", &self.bytes.len())
            .finish()
    }
}

/// Verified identity of one exact document in the FRED terms bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredTermsDocumentEvidence {
    role: FredTermsDocumentRole,
    representation: FredTermsDocumentRepresentation,
    url: String,
    digest: Sha256Digest,
    byte_length: usize,
    raw_digest: Option<Sha256Digest>,
    raw_byte_length: Option<usize>,
}

impl FredTermsDocumentEvidence {
    /// Returns the required document role.
    pub const fn role(&self) -> FredTermsDocumentRole {
        self.role
    }

    /// Returns the exact canonical document URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the verified reviewed-representation digest.
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the verified reviewed-representation byte length.
    pub const fn byte_length(&self) -> usize {
        self.byte_length
    }

    /// Returns the exact supplied raw-body digest after body verification.
    pub const fn raw_digest(&self) -> Option<Sha256Digest> {
        self.raw_digest
    }

    /// Returns the exact supplied raw-body length after body verification.
    pub const fn raw_byte_length(&self) -> Option<usize> {
        self.raw_byte_length
    }
}

/// Exact reviewed terms-document bundle and its local revalidation window.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredTermsEvidence {
    documents: BTreeMap<FredTermsDocumentRole, FredTermsDocumentEvidence>,
    bundle_digest: Sha256Digest,
    assessed_at: Timestamp,
    review_required_by: Timestamp,
}

impl FredTermsEvidence {
    /// Returns the deterministic digest of all exact reviewed terms documents.
    pub const fn bundle_digest(&self) -> Sha256Digest {
        self.bundle_digest
    }

    /// Returns the verified documents in deterministic role order.
    pub fn documents(&self) -> impl ExactSizeIterator<Item = &FredTermsDocumentEvidence> {
        self.documents.values()
    }

    /// Returns evidence for one required document role.
    pub fn document(&self, role: FredTermsDocumentRole) -> Option<&FredTermsDocumentEvidence> {
        self.documents.get(&role)
    }

    /// Returns when the local assessment was made.
    pub const fn assessed_at(&self) -> Timestamp {
        self.assessed_at
    }

    /// Returns the local fail-closed revalidation deadline.
    pub const fn review_required_by(&self) -> Timestamp {
        self.review_required_by
    }
}

/// A release evidence artifact describing the reviewed FRED rights state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredRightsArtifact {
    artifact_digest: Sha256Digest,
    terms: FredTermsEvidence,
    operations: Vec<FredOperation>,
    disposition: FredRightsDisposition,
}

impl FredRightsArtifact {
    /// Parses the release artifact and verifies every exact reviewed terms document.
    ///
    /// # Errors
    ///
    /// Returns a typed rights error when the artifact, required document set, exact bytes,
    /// deterministic bundle digest, source evidence, or fail-closed decision is invalid.
    pub fn parse(
        bytes: &[u8],
        terms_bytes: &[FredTermsDocumentBytes<'_>],
    ) -> Result<Self, FredRightsError> {
        let mut artifact = Self::parse_manifest(bytes)?;
        artifact.terms.documents =
            verify_terms_document_bytes(&artifact.terms.documents, terms_bytes)?;
        Ok(artifact)
    }

    /// Parses a reviewed manifest only when its exact caller-pinned SHA-256 identity matches.
    ///
    /// This verifies the complete manifest structure and retained source identities. It does not
    /// claim that raw terms bodies were reacquired; release validation must use [`Self::parse`].
    pub fn parse_reviewed_manifest(
        bytes: &[u8],
        expected_artifact_digest: Sha256Digest,
    ) -> Result<Self, FredRightsError> {
        if expected_artifact_digest.is_zero() || Sha256Digest::of(bytes) != expected_artifact_digest
        {
            return Err(FredRightsError::ArtifactIdentityMismatch);
        }
        Self::parse_manifest(bytes)
    }

    /// Parses only the exact code-reviewed current FRED rights manifest.
    pub fn parse_current_reviewed_manifest(bytes: &[u8]) -> Result<Self, FredRightsError> {
        if bytes.len() != CURRENT_FRED_RIGHTS_ARTIFACT_BYTE_LENGTH
            || Sha256Digest::of(bytes) != CURRENT_FRED_RIGHTS_ARTIFACT_SHA256
        {
            return Err(FredRightsError::ArtifactIdentityMismatch);
        }
        Self::current_reviewed()
    }

    /// Parses the exact current manifest and independently verifies all three raw terms bodies.
    pub fn parse_current_reviewed(
        bytes: &[u8],
        terms_bytes: &[FredTermsDocumentBytes<'_>],
    ) -> Result<Self, FredRightsError> {
        let mut artifact = Self::parse_current_reviewed_manifest(bytes)?;
        artifact.terms.documents =
            verify_terms_document_bytes(&artifact.terms.documents, terms_bytes)?;
        Ok(artifact)
    }

    /// Constructs the complete code-owned current manifest authority without a user file.
    pub fn current_reviewed() -> Result<Self, FredRightsError> {
        let mut documents = BTreeMap::new();
        for (role, representation, digest, byte_length) in [
            (
                FredTermsDocumentRole::ApiTerms,
                FredTermsDocumentRepresentation::ExactRaw,
                CURRENT_API_TERMS_SHA256,
                48_783,
            ),
            (
                FredTermsDocumentRole::FredServicesLegalTerms,
                FredTermsDocumentRepresentation::ExactRaw,
                CURRENT_LEGAL_TERMS_SHA256,
                116_858,
            ),
            (
                FredTermsDocumentRole::PrivacyPolicy,
                FredTermsDocumentRepresentation::PrivacySxaSearchItemCanonicalV1,
                CURRENT_PRIVACY_SHA256,
                246_317,
            ),
        ] {
            let evidence = FredTermsDocumentEvidence {
                role,
                representation,
                url: role.canonical_url().to_owned(),
                digest,
                byte_length,
                raw_digest: None,
                raw_byte_length: None,
            };
            if documents.insert(role, evidence).is_some() {
                return Err(FredRightsError::InvalidArtifact(
                    "duplicate current terms document",
                ));
            }
        }
        let terms = FredTermsEvidence {
            documents,
            bundle_digest: CURRENT_TERMS_BUNDLE_SHA256,
            assessed_at: Timestamp::from_unix_nanos(CURRENT_TERMS_ASSESSED_AT_UNIX_NANOS),
            review_required_by: Timestamp::from_unix_nanos(
                CURRENT_TERMS_REVIEW_REQUIRED_BY_UNIX_NANOS,
            ),
        };
        validate_terms(&terms)?;
        Ok(Self {
            artifact_digest: CURRENT_FRED_RIGHTS_ARTIFACT_SHA256,
            terms,
            operations: CURRENT_DURABLE_OPERATIONS.to_vec(),
            disposition: FredRightsDisposition::ServicePermissionRequired,
        })
    }

    fn parse_manifest(bytes: &[u8]) -> Result<Self, FredRightsError> {
        if bytes.len() > 256 * 1024 {
            return Err(FredRightsError::ArtifactTooLarge);
        }
        let wire: ArtifactWire = serde_json::from_slice(bytes)?;
        if wire.schema_version != 5 || wire.series_scope != "exact_service_and_series_grants" {
            return Err(FredRightsError::InvalidArtifact("schema or series scope"));
        }
        let documents = parse_terms_documents(&wire.terms_documents)?;
        let terms = FredTermsEvidence {
            bundle_digest: Sha256Digest::from_lower_hex(&wire.terms_bundle_digest)?,
            documents,
            assessed_at: Timestamp::from_unix_nanos(wire.assessed_at_unix_nanos),
            review_required_by: Timestamp::from_unix_nanos(wire.review_required_by_unix_nanos),
        };
        validate_terms(&terms)?;
        if terms.bundle_digest != terms_bundle_digest(&terms.documents)? {
            return Err(FredRightsError::TermsEvidenceMismatch);
        }
        validate_operations(&wire.operations)?;
        if wire.operations.contains(&FredOperation::RetrieveEphemeral)
            || wire.disposition != FredRightsDisposition::ServicePermissionRequired
            || wire.confirmed_facts.is_empty()
            || wire.engineering_inferences.is_empty()
            || wire.sources.is_empty()
        {
            return Err(FredRightsError::InvalidArtifact("rights decision"));
        }
        for source in &wire.sources {
            validate_https_url(&source.url)?;
        }
        let all_documents_confirmed = terms.documents().all(|document| {
            wire.sources.iter().any(|source| {
                source.url == document.url
                    && source_identity_supports_document(source, document)
                    && source.evidence_class == "confirmed"
                    && !source.accessed_on.is_empty()
            })
        });
        if !all_documents_confirmed {
            return Err(FredRightsError::InvalidArtifact("terms source evidence"));
        }
        Ok(Self {
            artifact_digest: Sha256Digest::of(bytes),
            terms,
            operations: wire.operations,
            disposition: wire.disposition,
        })
    }

    /// Returns the exact release-manifest digest.
    pub const fn artifact_digest(&self) -> Sha256Digest {
        self.artifact_digest
    }

    /// Returns the reviewed terms evidence.
    pub const fn terms_evidence(&self) -> &FredTermsEvidence {
        &self.terms
    }

    /// Returns the assessment timestamp.
    pub const fn assessed_at(&self) -> Timestamp {
        self.terms.assessed_at
    }

    /// Returns the local fail-closed review deadline.
    pub const fn review_required_by(&self) -> Timestamp {
        self.terms.review_required_by
    }

    /// Returns the operations assessed by the release artifact.
    pub fn operations(&self) -> &[FredOperation] {
        &self.operations
    }

    /// Returns the release disposition.
    pub const fn disposition(&self) -> FredRightsDisposition {
        self.disposition
    }
}

/// Independently verified exact owner-authorization evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredOwnerAuthorizationEvidence {
    url: String,
    digest: Sha256Digest,
    byte_length: usize,
}

impl FredOwnerAuthorizationEvidence {
    /// Verifies declared evidence identity against the exact supplied authorization bytes.
    pub fn try_new(
        url: String,
        digest: Sha256Digest,
        byte_length: usize,
        authorization_bytes: &[u8],
    ) -> Result<Self, FredRightsError> {
        validate_https_url(&url)?;
        if digest.is_zero()
            || byte_length == 0
            || byte_length != authorization_bytes.len()
            || digest != Sha256Digest::of(authorization_bytes)
        {
            return Err(FredRightsError::OwnerEvidenceMismatch);
        }
        Ok(Self {
            url,
            digest,
            byte_length,
        })
    }

    /// Returns the exact evidence URL.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Returns the verified authorization digest.
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the verified authorization byte length.
    pub const fn byte_length(&self) -> usize {
        self.byte_length
    }
}

/// Closed authority basis for one exact FRED series.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FredSeriesRightsBasis {
    /// The series owner has published evidence placing the exact material in the public domain.
    PublicDomain,
    /// The series owner supplied explicit permission for the retained operations.
    OwnerPermission,
}

/// Exact, typed evidence supporting one series-rights basis.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredSeriesRightsEvidence {
    series: SourceIdentifier,
    owner: SourceIdentifier,
    basis: FredSeriesRightsBasis,
    evidence_reference_url: String,
    authority_url: String,
    digest: Sha256Digest,
    byte_length: usize,
    effective_at: Timestamp,
    expires_at: Timestamp,
}

impl FredSeriesRightsEvidence {
    /// Verifies exact public-domain evidence for one exact series and validity interval.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact evidence identity and validity remain explicit"
    )]
    pub fn try_public_domain(
        series: SourceIdentifier,
        owner: SourceIdentifier,
        evidence_reference_url: String,
        authority_url: String,
        digest: Sha256Digest,
        byte_length: usize,
        evidence_bytes: &[u8],
        effective_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FredRightsError> {
        Self::try_exact(
            series,
            owner,
            FredSeriesRightsBasis::PublicDomain,
            evidence_reference_url,
            authority_url,
            digest,
            byte_length,
            evidence_bytes,
            effective_at,
            expires_at,
        )
    }

    /// Verifies exact custom owner-permission bytes and their explicit validity interval.
    #[allow(
        clippy::too_many_arguments,
        reason = "exact evidence identity and validity remain explicit"
    )]
    pub fn try_owner_permission(
        series: SourceIdentifier,
        owner: SourceIdentifier,
        evidence_reference_url: String,
        authority_url: String,
        digest: Sha256Digest,
        byte_length: usize,
        evidence_bytes: &[u8],
        effective_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FredRightsError> {
        Self::try_exact(
            series,
            owner,
            FredSeriesRightsBasis::OwnerPermission,
            evidence_reference_url,
            authority_url,
            digest,
            byte_length,
            evidence_bytes,
            effective_at,
            expires_at,
        )
    }

    /// Parses only the exact code-reviewed BLS public-domain decision for the `UNRATE` starter.
    pub fn parse_reviewed_unrate_public_domain(bytes: &[u8]) -> Result<Self, FredRightsError> {
        if bytes.len() != CURRENT_UNRATE_RIGHTS_ARTIFACT_BYTE_LENGTH
            || Sha256Digest::of(bytes) != CURRENT_UNRATE_RIGHTS_ARTIFACT_SHA256
        {
            return Err(FredRightsError::ArtifactIdentityMismatch);
        }
        validate_https_url(CURRENT_UNRATE_EVIDENCE_REFERENCE)?;
        validate_https_url(CURRENT_UNRATE_AUTHORITY_URL)?;
        Ok(Self {
            series: SourceIdentifier::try_from("UNRATE")
                .map_err(|_| FredRightsError::InvalidSeries)?,
            owner: SourceIdentifier::try_from("us-bureau-of-labor-statistics")
                .map_err(|_| FredRightsError::InvalidRightsOwner)?,
            basis: FredSeriesRightsBasis::PublicDomain,
            evidence_reference_url: CURRENT_UNRATE_EVIDENCE_REFERENCE.to_owned(),
            authority_url: CURRENT_UNRATE_AUTHORITY_URL.to_owned(),
            digest: CURRENT_UNRATE_RIGHTS_ARTIFACT_SHA256,
            byte_length: CURRENT_UNRATE_RIGHTS_ARTIFACT_BYTE_LENGTH,
            effective_at: Timestamp::from_unix_nanos(1_785_024_000_000_000_000),
            expires_at: Timestamp::from_unix_nanos(CURRENT_TERMS_REVIEW_REQUIRED_BY_UNIX_NANOS),
        })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "exact evidence identity and validity remain explicit"
    )]
    fn try_exact(
        series: SourceIdentifier,
        owner: SourceIdentifier,
        basis: FredSeriesRightsBasis,
        evidence_reference_url: String,
        authority_url: String,
        digest: Sha256Digest,
        byte_length: usize,
        evidence_bytes: &[u8],
        effective_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FredRightsError> {
        validate_exact_series_id(series.as_str())?;
        validate_exact_rights_owner(owner.as_str())?;
        validate_https_url(&evidence_reference_url)?;
        validate_https_url(&authority_url)?;
        if digest.is_zero()
            || byte_length == 0
            || byte_length > MAX_FRED_SERIES_RIGHTS_EVIDENCE_BYTES
            || byte_length != evidence_bytes.len()
            || digest != Sha256Digest::of(evidence_bytes)
            || effective_at >= expires_at
        {
            return Err(FredRightsError::SeriesEvidenceMismatch);
        }
        Ok(Self {
            series,
            owner,
            basis,
            evidence_reference_url,
            authority_url,
            digest,
            byte_length,
            effective_at,
            expires_at,
        })
    }

    fn from_verified_owner(
        series: SourceIdentifier,
        owner: SourceIdentifier,
        authorization: FredOwnerAuthorizationEvidence,
        effective_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FredRightsError> {
        validate_exact_series_id(series.as_str())?;
        validate_exact_rights_owner(owner.as_str())?;
        if effective_at >= expires_at {
            return Err(FredRightsError::InvalidGrant);
        }
        Ok(Self {
            series,
            owner,
            basis: FredSeriesRightsBasis::OwnerPermission,
            evidence_reference_url: authorization.url.clone(),
            authority_url: authorization.url,
            digest: authorization.digest,
            byte_length: authorization.byte_length,
            effective_at,
            expires_at,
        })
    }

    /// Returns the exact series covered by this evidence.
    pub const fn series(&self) -> &SourceIdentifier {
        &self.series
    }

    /// Returns the exact series-owner identity covered by this evidence.
    pub const fn owner(&self) -> &SourceIdentifier {
        &self.owner
    }

    /// Returns the closed rights basis.
    pub const fn basis(&self) -> FredSeriesRightsBasis {
        self.basis
    }

    /// Returns the exact reference whose bytes the digest and length identify.
    pub fn evidence_reference_url(&self) -> &str {
        &self.evidence_reference_url
    }

    /// Returns the authoritative owner rights source.
    pub fn authority_url(&self) -> &str {
        &self.authority_url
    }

    /// Returns the exact evidence digest.
    pub const fn digest(&self) -> Sha256Digest {
        self.digest
    }

    /// Returns the exact evidence byte length.
    pub const fn byte_length(&self) -> usize {
        self.byte_length
    }

    /// Returns the inclusive evidence-validity start.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns the exclusive evidence review or permission expiry.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Authentic delivery channel retained for one exact St. Louis Fed permission response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredServicePermissionChannel {
    evidence_url: String,
    authority_url: String,
}

impl FredServicePermissionChannel {
    /// Verifies an exact permission document delivered from official St. Louis Fed HTTPS hosts.
    pub fn try_official_https(
        evidence_url: String,
        authority_url: String,
    ) -> Result<Self, FredRightsError> {
        validate_st_louis_fed_authority_url(&evidence_url)?;
        validate_st_louis_fed_authority_url(&authority_url)?;
        Ok(Self {
            evidence_url,
            authority_url,
        })
    }
}

/// Explicit local legal/rights review bound to one exact Bank response.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredServicePermissionReview {
    reviewer: SourceIdentifier,
    reviewed_at: Timestamp,
    issuer: SourceIdentifier,
    application: SourceIdentifier,
    service: SourceIdentifier,
    series: BTreeSet<SourceIdentifier>,
    operations: BTreeSet<FredOperation>,
    conditions: Vec<String>,
    effective_at: Timestamp,
    expires_at: Option<Timestamp>,
    revalidate_by: Timestamp,
}

impl FredServicePermissionReview {
    /// Constructs a bounded explicit review decision for one exact response document.
    ///
    /// # Errors
    ///
    /// Rejects untrusted identities, empty or duplicated scope, invalid conditions, and
    /// inconsistent effective, expiry, review, or revalidation timestamps.
    #[allow(
        clippy::too_many_arguments,
        reason = "reviewer, confirmed authority scope, conditions, and validity are independent"
    )]
    pub fn try_new(
        reviewer: SourceIdentifier,
        reviewed_at: Timestamp,
        issuer: SourceIdentifier,
        application: SourceIdentifier,
        service: SourceIdentifier,
        series: Vec<SourceIdentifier>,
        operations: Vec<FredOperation>,
        conditions: Vec<String>,
        effective_at: Timestamp,
        expires_at: Option<Timestamp>,
        revalidate_by: Timestamp,
    ) -> Result<Self, FredRightsError> {
        validate_service_operations(&operations)?;
        if reviewer.as_str().is_empty()
            || issuer.as_str() != SERVICE_PERMISSION_ISSUER
            || application.as_str() != SERVICE_PERMISSION_APPLICATION
            || service.as_str() != SERVICE_PERMISSION_SERVICE
            || series.is_empty()
            || series.len() > 256
            || reviewed_at < effective_at
            || effective_at >= revalidate_by
            || reviewed_at >= revalidate_by
            || expires_at.is_some_and(|expires_at| effective_at >= expires_at)
            || conditions.len() > 32
            || conditions
                .iter()
                .any(|condition| condition.is_empty() || condition.len() > 1_024)
            || conditions.iter().collect::<BTreeSet<_>>().len() != conditions.len()
        {
            return Err(FredRightsError::InvalidServicePermission);
        }
        for exact_series in &series {
            validate_exact_series_id(exact_series.as_str())?;
        }
        let series_count = series.len();
        let series_set = series.into_iter().collect::<BTreeSet<_>>();
        if series_set.len() != series_count {
            return Err(FredRightsError::InvalidServicePermission);
        }
        let operation_set = operations.into_iter().collect::<BTreeSet<_>>();
        Ok(Self {
            reviewer,
            reviewed_at,
            issuer,
            application,
            service,
            series: series_set,
            operations: operation_set,
            conditions,
            effective_at,
            expires_at,
            revalidate_by,
        })
    }
}

/// Exact written Federal Reserve Bank of St. Louis permission for Market Squawk's FRED API use.
///
/// This is independent of the underlying series owner's rights. It combines authentic raw
/// response evidence with an explicit local review decision for the exact durable scope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredServicePermissionEvidence {
    channel: FredServicePermissionChannel,
    review: FredServicePermissionReview,
    terms_bundle_digest: Sha256Digest,
    document_digest: Sha256Digest,
    binding_digest: Sha256Digest,
    byte_length: usize,
}

impl FredServicePermissionEvidence {
    /// Binds exact raw Bank-response bytes to an explicit reviewed service-permission decision.
    ///
    /// # Errors
    ///
    /// Returns a typed rights error when document identity, terms binding, or exact raw bytes are
    /// absent or inconsistent.
    pub fn try_new(
        channel: FredServicePermissionChannel,
        review: FredServicePermissionReview,
        terms_bundle_digest: Sha256Digest,
        document_digest: Sha256Digest,
        byte_length: usize,
        permission_bytes: &[u8],
    ) -> Result<Self, FredRightsError> {
        if terms_bundle_digest.is_zero()
            || document_digest.is_zero()
            || byte_length == 0
            || byte_length > MAX_FRED_SERVICE_PERMISSION_BYTES
            || byte_length != permission_bytes.len()
            || document_digest != Sha256Digest::of(permission_bytes)
        {
            return Err(FredRightsError::ServicePermissionMismatch);
        }
        let binding_digest = service_permission_binding_digest(ServicePermissionBinding {
            channel: &channel,
            review: &review,
            terms_bundle_digest,
            document_digest,
            byte_length,
        })?;
        Ok(Self {
            channel,
            review,
            terms_bundle_digest,
            document_digest,
            binding_digest,
            byte_length,
        })
    }

    /// Returns the authentic raw-response channel.
    pub const fn channel(&self) -> &FredServicePermissionChannel {
        &self.channel
    }

    /// Returns the explicit local review decision.
    pub const fn review(&self) -> &FredServicePermissionReview {
        &self.review
    }

    /// Returns the exact reviewed terms bundle to which the permission is bound.
    pub const fn terms_bundle_digest(&self) -> Sha256Digest {
        self.terms_bundle_digest
    }

    /// Returns the exact permission-document digest.
    pub const fn document_digest(&self) -> Sha256Digest {
        self.document_digest
    }

    /// Returns the digest binding raw evidence, review scope, terms, and validity.
    pub const fn binding_digest(&self) -> Sha256Digest {
        self.binding_digest
    }

    /// Returns the exact permission-document byte length.
    pub const fn byte_length(&self) -> usize {
        self.byte_length
    }

    /// Returns the inclusive reviewed permission-validity start.
    pub const fn effective_at(&self) -> Timestamp {
        self.review.effective_at
    }

    /// Returns the document-stated expiry, when the exact permission has one.
    pub const fn expires_at(&self) -> Option<Timestamp> {
        self.review.expires_at
    }

    /// Returns the mandatory finite local revalidation deadline.
    pub const fn revalidate_by(&self) -> Timestamp {
        self.review.revalidate_by
    }
}

/// An exact-series authorization supported by typed owner evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredSeriesRightsGrant {
    series: SourceIdentifier,
    owner: SourceIdentifier,
    evidence: FredSeriesRightsEvidence,
    citation_url: String,
    terms_bundle_digest: Sha256Digest,
    operations: BTreeSet<FredOperation>,
    effective_at: Timestamp,
    expires_at: Timestamp,
}

impl FredSeriesRightsGrant {
    /// Builds a bounded exact-series grant; wildcard grants are not supported.
    #[allow(
        clippy::too_many_arguments,
        reason = "all evidence fields bind rights authority"
    )]
    pub fn try_new(
        series: SourceIdentifier,
        owner: SourceIdentifier,
        authorization: FredOwnerAuthorizationEvidence,
        terms_bundle_digest: Sha256Digest,
        operations: Vec<FredOperation>,
        effective_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FredRightsError> {
        let evidence = FredSeriesRightsEvidence::from_verified_owner(
            series.clone(),
            owner.clone(),
            authorization,
            effective_at,
            expires_at,
        )?;
        Self::try_new_with_evidence(
            series,
            owner,
            evidence,
            terms_bundle_digest,
            operations,
            effective_at,
            expires_at,
        )
    }

    /// Builds a bounded exact-series grant from a typed public-domain or owner-permission basis.
    #[allow(
        clippy::too_many_arguments,
        reason = "all evidence fields bind rights authority"
    )]
    pub fn try_new_with_evidence(
        series: SourceIdentifier,
        owner: SourceIdentifier,
        evidence: FredSeriesRightsEvidence,
        terms_bundle_digest: Sha256Digest,
        operations: Vec<FredOperation>,
        effective_at: Timestamp,
        expires_at: Timestamp,
    ) -> Result<Self, FredRightsError> {
        validate_operations(&operations)?;
        validate_exact_series_id(series.as_str())?;
        validate_exact_rights_owner(owner.as_str())?;
        if evidence.series != series
            || evidence.owner != owner
            || terms_bundle_digest.is_zero()
            || effective_at >= expires_at
            || effective_at < evidence.effective_at
            || expires_at > evidence.expires_at
        {
            return Err(FredRightsError::InvalidGrant);
        }
        let citation_url = format!("{CURRENT_UNRATE_CITATION_URL_PREFIX}{}", series.as_str());
        validate_https_url(&citation_url)?;
        Ok(Self {
            series,
            owner,
            evidence,
            citation_url,
            terms_bundle_digest,
            operations: operations.into_iter().collect(),
            effective_at,
            expires_at,
        })
    }

    /// Returns the exact covered series.
    pub const fn series(&self) -> &SourceIdentifier {
        &self.series
    }

    /// Returns the series-owner identity bound to the grant.
    pub const fn owner(&self) -> &SourceIdentifier {
        &self.owner
    }

    /// Returns the exact evidence reference URL.
    pub fn authorization_url(&self) -> &str {
        self.evidence.evidence_reference_url()
    }

    /// Returns the digest of the exact series-rights evidence bytes.
    pub const fn authorization_digest(&self) -> Sha256Digest {
        self.evidence.digest()
    }

    /// Returns the verified series-rights evidence byte length.
    pub const fn authorization_byte_length(&self) -> usize {
        self.evidence.byte_length()
    }

    /// Returns the typed rights evidence retained by the grant.
    pub const fn evidence(&self) -> &FredSeriesRightsEvidence {
        &self.evidence
    }

    /// Returns the closed public-domain or owner-permission basis.
    pub const fn rights_basis(&self) -> FredSeriesRightsBasis {
        self.evidence.basis()
    }

    /// Returns the authoritative owner rights source.
    pub fn authority_url(&self) -> &str {
        self.evidence.authority_url()
    }

    /// Returns the canonical FRED series citation.
    pub fn citation_url(&self) -> &str {
        &self.citation_url
    }

    /// Returns the complete FRED terms-bundle digest to which this grant is bound.
    pub const fn terms_bundle_digest(&self) -> Sha256Digest {
        self.terms_bundle_digest
    }

    /// Returns the operations affirmatively granted.
    pub fn operations(&self) -> impl Iterator<Item = FredOperation> + '_ {
        self.operations.iter().copied()
    }

    /// Returns the inclusive grant start instant.
    pub const fn effective_at(&self) -> Timestamp {
        self.effective_at
    }

    /// Returns the exclusive grant expiry instant.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }
}

/// Runtime fail-closed FRED rights authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredDurableAuthority {
    terms_reference: String,
    terms_digest: Sha256Digest,
    authorization_digest: Sha256Digest,
    expires_at: Timestamp,
    admitted_operations: BTreeMap<SourceIdentifier, BTreeSet<FredOperation>>,
}

impl FredDurableAuthority {
    /// Returns the exact reviewed API-terms document represented by the generic persistence basis.
    pub fn terms_reference(&self) -> &str {
        &self.terms_reference
    }

    /// Returns the exact reviewed API-terms document digest.
    pub const fn terms_digest(&self) -> Sha256Digest {
        self.terms_digest
    }

    /// Returns the canonical two-gate authority digest for this complete exact-series scope.
    pub const fn authorization_digest(&self) -> Sha256Digest {
        self.authorization_digest
    }

    /// Returns the earliest exclusive expiry across terms, Bank permission, and series grants.
    pub const fn expires_at(&self) -> Timestamp {
        self.expires_at
    }

    /// Returns exact series in deterministic order.
    pub fn series(&self) -> impl ExactSizeIterator<Item = &SourceIdentifier> {
        self.admitted_operations.keys()
    }

    /// Returns whether one operation is affirmatively admitted for one exact series.
    pub fn admits(&self, series: &SourceIdentifier, operation: FredOperation) -> bool {
        self.admitted_operations
            .get(series)
            .is_some_and(|operations| operations.contains(&operation))
    }
}

/// Runtime fail-closed FRED rights policy.
#[derive(Clone, Debug)]
pub struct FredRightsPolicy {
    artifact: FredRightsArtifact,
    service_permission: Option<FredServicePermissionEvidence>,
    grants: BTreeMap<SourceIdentifier, FredSeriesRightsGrant>,
}

impl FredRightsPolicy {
    /// Builds a policy bound to current terms, optional exact Bank permission, and series grants.
    pub fn try_new(
        artifact: FredRightsArtifact,
        service_permission: Option<FredServicePermissionEvidence>,
        grants: Vec<FredSeriesRightsGrant>,
    ) -> Result<Self, FredRightsError> {
        validate_terms(&artifact.terms)?;
        validate_operations(&artifact.operations)?;
        let artifact_operations: BTreeSet<_> = artifact.operations.iter().copied().collect();
        if let Some(permission) = &service_permission
            && (permission.terms_bundle_digest != artifact.terms.bundle_digest
                || permission.review.reviewed_at < artifact.terms.assessed_at
                || permission.review.revalidate_by > artifact.terms.review_required_by
                || permission
                    .review
                    .operations
                    .iter()
                    .any(|operation| !artifact_operations.contains(operation)))
        {
            return Err(FredRightsError::InvalidServicePermission);
        }
        let mut by_series = BTreeMap::new();
        for grant in grants {
            if grant.terms_bundle_digest != artifact.terms.bundle_digest
                || grant.expires_at > artifact.terms.review_required_by
                || grant
                    .operations
                    .iter()
                    .any(|operation| !artifact_operations.contains(operation))
                || service_permission.as_ref().is_some_and(|permission| {
                    grant
                        .operations
                        .iter()
                        .any(|operation| requires_service_permission(*operation))
                        && !permission.review.series.contains(&grant.series)
                })
                || by_series.insert(grant.series.clone(), grant).is_some()
            {
                return Err(FredRightsError::InvalidGrant);
            }
        }
        Ok(Self {
            artifact,
            service_permission,
            grants: by_series,
        })
    }

    /// Returns the complete reviewed artifact bound into this policy.
    pub const fn artifact(&self) -> &FredRightsArtifact {
        &self.artifact
    }

    /// Assesses exact operations for one exact series at a caller-supplied time.
    pub fn assess(
        &self,
        series: &SourceIdentifier,
        operations: &[FredOperation],
        at: Timestamp,
    ) -> Result<FredRightsDecision, FredRightsError> {
        validate_exact_series_id(series.as_str())?;
        validate_operations(operations)?;
        let terms = &self.artifact.terms;
        let artifact_operations: BTreeSet<_> = self.artifact.operations.iter().copied().collect();
        let disposition = if at < terms.assessed_at || at >= terms.review_required_by {
            FredRightsDisposition::BlockedStaleTerms
        } else if operations == [FredOperation::RetrieveEphemeral] {
            FredRightsDisposition::Permitted
        } else if !has_verified_raw_terms_documents(terms) {
            FredRightsDisposition::BlockedUnknownRights
        } else if operations.contains(&FredOperation::RetrieveEphemeral)
            || operations
                .iter()
                .any(|operation| !artifact_operations.contains(operation))
        {
            FredRightsDisposition::BlockedOperationScope
        } else if let Some(grant) = self.grants.get(series) {
            if at < grant.effective_at || at >= grant.expires_at {
                FredRightsDisposition::BlockedUnknownRights
            } else if operations
                .iter()
                .any(|operation| !grant.operations.contains(operation))
            {
                FredRightsDisposition::BlockedOperationScope
            } else {
                let durable_operations = operations
                    .iter()
                    .copied()
                    .filter(|operation| requires_service_permission(*operation))
                    .collect::<Vec<_>>();
                if durable_operations.is_empty()
                    || self.service_permission.as_ref().is_some_and(|permission| {
                        permission.review.series.contains(series)
                            && at >= permission.review.effective_at
                            && at < permission.review.revalidate_by
                            && permission
                                .review
                                .expires_at
                                .is_none_or(|expires_at| at < expires_at)
                            && durable_operations
                                .iter()
                                .all(|operation| permission.review.operations.contains(operation))
                    })
                {
                    FredRightsDisposition::Permitted
                } else {
                    FredRightsDisposition::BlockedServicePermission
                }
            }
        } else {
            FredRightsDisposition::BlockedUnknownRights
        };
        Ok(FredRightsDecision {
            rights_artifact_digest: self.artifact.artifact_digest,
            terms_bundle_digest: terms.bundle_digest,
            operations: operations.to_vec(),
            review_required_by: terms.review_required_by,
            disposition,
        })
    }

    /// Derives the exact subordinate durable authority admitted at one instant.
    ///
    /// The code-owned profile remains globally conservative. This authority exists only when the
    /// reviewed terms, exact Bank permission, and every configured exact-series grant are current,
    /// and every retained series independently admits persistence.
    ///
    /// # Errors
    ///
    /// Returns [`FredRightsError::DurableAuthorityUnavailable`] whenever either authority gate is
    /// absent, stale, out of scope, or not persistence-capable.
    pub fn durable_authority(
        &self,
        at: Timestamp,
    ) -> Result<FredDurableAuthority, FredRightsError> {
        let terms = self.artifact.terms_evidence();
        if at < terms.assessed_at() || at >= terms.review_required_by() {
            return Err(FredRightsError::DurableAuthorityUnavailable);
        }
        if !has_verified_raw_terms_documents(terms) {
            return Err(FredRightsError::DurableAuthorityUnavailable);
        }
        let permission = self
            .service_permission
            .as_ref()
            .ok_or(FredRightsError::DurableAuthorityUnavailable)?;
        let permission_expiry = permission
            .expires_at()
            .map_or(permission.revalidate_by(), |expires_at| {
                expires_at.min(permission.revalidate_by())
            });
        if at < permission.effective_at() || at >= permission_expiry {
            return Err(FredRightsError::DurableAuthorityUnavailable);
        }
        let api_terms = terms
            .document(FredTermsDocumentRole::ApiTerms)
            .ok_or(FredRightsError::DurableAuthorityUnavailable)?;
        let mut expires_at = terms.review_required_by().min(permission_expiry);
        let mut admitted_operations = BTreeMap::new();
        for grant in self.grants.values() {
            if at < grant.effective_at() || at >= grant.expires_at() {
                return Err(FredRightsError::DurableAuthorityUnavailable);
            }
            let mut operations = BTreeSet::new();
            for operation in grant.operations() {
                if self.assess(grant.series(), &[operation], at)?.disposition()
                    == FredRightsDisposition::Permitted
                {
                    operations.insert(operation);
                }
            }
            if !operations.contains(&FredOperation::Persist)
                || admitted_operations
                    .insert(grant.series().clone(), operations)
                    .is_some()
            {
                return Err(FredRightsError::DurableAuthorityUnavailable);
            }
            expires_at = expires_at.min(grant.expires_at());
        }
        if admitted_operations.is_empty() || at >= expires_at {
            return Err(FredRightsError::DurableAuthorityUnavailable);
        }
        let authorization_digest =
            durable_authority_digest(self, permission, &admitted_operations)?;
        Ok(FredDurableAuthority {
            terms_reference: api_terms.url().to_owned(),
            terms_digest: api_terms.digest(),
            authorization_digest,
            expires_at,
            admitted_operations,
        })
    }
}

/// A complete result of one runtime rights assessment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FredRightsDecision {
    rights_artifact_digest: Sha256Digest,
    terms_bundle_digest: Sha256Digest,
    operations: Vec<FredOperation>,
    review_required_by: Timestamp,
    disposition: FredRightsDisposition,
}

impl FredRightsDecision {
    /// Returns the complete reviewed rights-artifact identity used for this decision.
    pub const fn rights_artifact_digest(&self) -> Sha256Digest {
        self.rights_artifact_digest
    }

    /// Returns the exact complete terms-bundle digest assessed.
    pub const fn terms_bundle_digest(&self) -> Sha256Digest {
        self.terms_bundle_digest
    }

    /// Returns the exact requested operations.
    pub fn operations(&self) -> &[FredOperation] {
        &self.operations
    }

    /// Returns the local terms revalidation deadline.
    pub const fn review_required_by(&self) -> Timestamp {
        self.review_required_by
    }

    /// Returns the fail-closed disposition.
    pub const fn disposition(&self) -> FredRightsDisposition {
        self.disposition
    }
}

/// A FRED rights evidence or policy validation failure.
#[derive(Debug, Error)]
pub enum FredRightsError {
    /// The artifact exceeds its fixed parser budget.
    #[error("FRED rights artifact exceeds the byte budget")]
    ArtifactTooLarge,
    /// The artifact is not valid JSON in the exact supported schema.
    #[error("invalid FRED rights artifact JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),
    /// A digest is not non-zero lowercase SHA-256 hexadecimal.
    #[error("invalid SHA-256 digest")]
    InvalidDigest,
    /// A structured artifact invariant failed.
    #[error("invalid FRED rights artifact: {0}")]
    InvalidArtifact(&'static str),
    /// A reviewed artifact did not match the exact caller- or code-pinned identity.
    #[error("FRED rights artifact identity does not match reviewed evidence")]
    ArtifactIdentityMismatch,
    /// Terms evidence is internally inconsistent or stale at construction.
    #[error("invalid FRED terms evidence")]
    InvalidTerms,
    /// One caller-supplied terms document is empty or exceeds its fixed byte ceiling.
    #[error("invalid FRED terms document bytes")]
    InvalidTermsDocumentBytes,
    /// Supplied terms bytes do not match the artifact's declared exact evidence.
    #[error("FRED terms bytes do not match the release artifact")]
    TermsEvidenceMismatch,
    /// A terms body does not contain exactly one valid instance of its declared representation.
    #[error("FRED terms body does not match its declared representation")]
    TermsDocumentRepresentationMismatch,
    /// Supplied owner-authorization bytes do not match their declared exact evidence.
    #[error("FRED owner authorization bytes do not match declared evidence")]
    OwnerEvidenceMismatch,
    /// Exact typed series-rights evidence did not match its declared bytes or validity.
    #[error("FRED series rights evidence does not match declared authority")]
    SeriesEvidenceMismatch,
    /// Exact Bank permission did not match its declared bytes, scope, or validity.
    #[error("FRED service permission does not match declared authority")]
    ServicePermissionMismatch,
    /// A series identifier was empty, wildcarded, oversized, or outside the provider grammar.
    #[error("invalid exact FRED series identifier")]
    InvalidSeries,
    /// A series-owner identifier was empty, wildcarded, oversized, or outside the local grammar.
    #[error("invalid exact FRED series-owner identifier")]
    InvalidRightsOwner,
    /// An exact-series grant is invalid or duplicates another grant.
    #[error("invalid FRED series rights grant")]
    InvalidGrant,
    /// The Bank service permission is not bound to the current terms and operation scope.
    #[error("invalid FRED service permission")]
    InvalidServicePermission,
    /// The operation set is empty or contains duplicates.
    #[error("invalid FRED operation set")]
    InvalidOperations,
    /// Exact terms, Bank permission, and per-series rights do not admit durable runtime authority.
    #[error("FRED durable authority is unavailable")]
    DurableAuthorityUnavailable,
    /// Evidence URL is not a valid HTTPS URL.
    #[error("rights evidence URL must be valid HTTPS")]
    InvalidUrl,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactWire {
    schema_version: u16,
    series_scope: String,
    terms_bundle_digest: String,
    terms_documents: Vec<TermsDocumentWire>,
    assessed_at_unix_nanos: i64,
    review_required_by_unix_nanos: i64,
    operations: Vec<FredOperation>,
    disposition: FredRightsDisposition,
    confirmed_facts: Vec<String>,
    engineering_inferences: Vec<String>,
    sources: Vec<SourceWire>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TermsDocumentWire {
    role: FredTermsDocumentRole,
    representation: FredTermsDocumentRepresentation,
    url: String,
    sha256: String,
    byte_length: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceWire {
    url: String,
    accessed_on: String,
    #[serde(default)]
    sha256: Option<String>,
    #[serde(default)]
    byte_length: Option<usize>,
    evidence_class: String,
}

const PRIVACY_SXA_SEARCH_ITEM_PREFIX: &[u8] = concat!(
    r#"<div class="component search-box col-12" id="a" data-properties='"#,
    r#"{"endpoint":"//sxa/search/results/","#,
    r#""suggestionEndpoint":"//sxa/search/suggestions/","suggestionsMode":"","#,
    r#""resultPage":"/search","targetSignature":"siteResults","#,
    r#""v":"{E22FB38C-3672-49E1-B145-563EEAEC4951}","#,
    r#""s":"{A10D94E2-3F41-4100-A3BA-24E58460A483}","#,
    r#""p":0,"l":"","languageSource":"AllLanguages","#,
    r#""searchResultsSignature":"","itemid":""#
)
.as_bytes();
const PRIVACY_SXA_SEARCH_ITEM_SUFFIX: &[u8] =
    r#"","minSuggestionsTriggerCharacterCount":2}'>"#.as_bytes();
const PRIVACY_SXA_SEARCH_ITEM_TOKEN: &[u8] = b"{VOLATILE-SXA-SEARCH-INSTANCE}";
const BRACED_GUID_BYTE_LENGTH: usize = 38;

const fn expected_representation(role: FredTermsDocumentRole) -> FredTermsDocumentRepresentation {
    match role {
        FredTermsDocumentRole::ApiTerms | FredTermsDocumentRole::FredServicesLegalTerms => {
            FredTermsDocumentRepresentation::ExactRaw
        }
        FredTermsDocumentRole::PrivacyPolicy => {
            FredTermsDocumentRepresentation::PrivacySxaSearchItemCanonicalV1
        }
    }
}

fn source_identity_supports_document(
    source: &SourceWire,
    document: &FredTermsDocumentEvidence,
) -> bool {
    let (Some(raw_digest), Some(raw_byte_length)) = (source.sha256.as_deref(), source.byte_length)
    else {
        return false;
    };
    let Ok(raw_digest) = Sha256Digest::from_lower_hex(raw_digest) else {
        return false;
    };
    if raw_byte_length == 0
        || raw_byte_length > MAX_FRED_TERMS_DOCUMENT_BYTES
        || raw_digest.is_zero()
    {
        return false;
    }
    match document.representation {
        FredTermsDocumentRepresentation::ExactRaw => {
            raw_byte_length == document.byte_length && raw_digest == document.digest
        }
        FredTermsDocumentRepresentation::PrivacySxaSearchItemCanonicalV1 => {
            let Some(raw_extra_bytes) =
                BRACED_GUID_BYTE_LENGTH.checked_sub(PRIVACY_SXA_SEARCH_ITEM_TOKEN.len())
            else {
                return false;
            };
            document.byte_length.checked_add(raw_extra_bytes) == Some(raw_byte_length)
        }
    }
}

fn reviewed_document_bytes<'a>(
    representation: FredTermsDocumentRepresentation,
    raw: &'a [u8],
) -> Result<Cow<'a, [u8]>, FredRightsError> {
    match representation {
        FredTermsDocumentRepresentation::ExactRaw => Ok(Cow::Borrowed(raw)),
        FredTermsDocumentRepresentation::PrivacySxaSearchItemCanonicalV1 => {
            canonicalize_privacy_sxa_search_item(raw).map(Cow::Owned)
        }
    }
}

fn canonicalize_privacy_sxa_search_item(raw: &[u8]) -> Result<Vec<u8>, FredRightsError> {
    let mut matches = raw
        .windows(PRIVACY_SXA_SEARCH_ITEM_PREFIX.len())
        .enumerate()
        .filter(|(_, candidate)| *candidate == PRIVACY_SXA_SEARCH_ITEM_PREFIX);
    let Some((component_start, _)) = matches.next() else {
        return Err(FredRightsError::TermsDocumentRepresentationMismatch);
    };
    if matches.next().is_some() {
        return Err(FredRightsError::TermsDocumentRepresentationMismatch);
    }

    let guid_start = component_start
        .checked_add(PRIVACY_SXA_SEARCH_ITEM_PREFIX.len())
        .ok_or(FredRightsError::TermsDocumentRepresentationMismatch)?;
    let guid_end = guid_start
        .checked_add(BRACED_GUID_BYTE_LENGTH)
        .ok_or(FredRightsError::TermsDocumentRepresentationMismatch)?;
    let guid = raw
        .get(guid_start..guid_end)
        .ok_or(FredRightsError::TermsDocumentRepresentationMismatch)?;
    if !is_uppercase_braced_guid(guid) {
        return Err(FredRightsError::TermsDocumentRepresentationMismatch);
    }
    let suffix_end = guid_end
        .checked_add(PRIVACY_SXA_SEARCH_ITEM_SUFFIX.len())
        .ok_or(FredRightsError::TermsDocumentRepresentationMismatch)?;
    if raw.get(guid_end..suffix_end) != Some(PRIVACY_SXA_SEARCH_ITEM_SUFFIX) {
        return Err(FredRightsError::TermsDocumentRepresentationMismatch);
    }

    let canonical_length = raw
        .len()
        .checked_sub(BRACED_GUID_BYTE_LENGTH)
        .and_then(|length| length.checked_add(PRIVACY_SXA_SEARCH_ITEM_TOKEN.len()))
        .ok_or(FredRightsError::TermsDocumentRepresentationMismatch)?;
    let mut canonical = Vec::new();
    canonical
        .try_reserve_exact(canonical_length)
        .map_err(|_| FredRightsError::InvalidTermsDocumentBytes)?;
    canonical.extend_from_slice(&raw[..guid_start]);
    canonical.extend_from_slice(PRIVACY_SXA_SEARCH_ITEM_TOKEN);
    canonical.extend_from_slice(&raw[guid_end..]);
    Ok(canonical)
}

fn is_uppercase_braced_guid(value: &[u8]) -> bool {
    if value.len() != BRACED_GUID_BYTE_LENGTH || value[0] != b'{' || value[37] != b'}' {
        return false;
    }
    value[1..37].iter().enumerate().all(|(index, byte)| {
        if matches!(index, 8 | 13 | 18 | 23) {
            *byte == b'-'
        } else {
            byte.is_ascii_digit() || (b'A'..=b'F').contains(byte)
        }
    })
}

fn parse_terms_documents(
    declared: &[TermsDocumentWire],
) -> Result<BTreeMap<FredTermsDocumentRole, FredTermsDocumentEvidence>, FredRightsError> {
    if declared.len() != FredTermsDocumentRole::ALL.len() {
        return Err(FredRightsError::TermsEvidenceMismatch);
    }
    let mut documents = BTreeMap::new();
    for document in declared {
        if document.url != document.role.canonical_url()
            || document.representation != expected_representation(document.role)
            || document.byte_length == 0
            || document.byte_length > MAX_FRED_TERMS_DOCUMENT_BYTES
        {
            return Err(FredRightsError::InvalidArtifact("terms document identity"));
        }
        let digest = Sha256Digest::from_lower_hex(&document.sha256)?;
        if digest.is_zero() {
            return Err(FredRightsError::InvalidArtifact("terms document digest"));
        }
        let evidence = FredTermsDocumentEvidence {
            role: document.role,
            representation: document.representation,
            url: document.url.clone(),
            digest,
            byte_length: document.byte_length,
            raw_digest: None,
            raw_byte_length: None,
        };
        if documents.insert(document.role, evidence).is_some() {
            return Err(FredRightsError::TermsEvidenceMismatch);
        }
    }
    if FredTermsDocumentRole::ALL
        .iter()
        .any(|role| !documents.contains_key(role))
    {
        return Err(FredRightsError::TermsEvidenceMismatch);
    }
    Ok(documents)
}

fn verify_terms_document_bytes(
    declared: &BTreeMap<FredTermsDocumentRole, FredTermsDocumentEvidence>,
    supplied: &[FredTermsDocumentBytes<'_>],
) -> Result<BTreeMap<FredTermsDocumentRole, FredTermsDocumentEvidence>, FredRightsError> {
    if declared.len() != FredTermsDocumentRole::ALL.len()
        || supplied.len() != FredTermsDocumentRole::ALL.len()
    {
        return Err(FredRightsError::TermsEvidenceMismatch);
    }
    let mut supplied_by_role = BTreeMap::new();
    for document in supplied {
        if document.bytes.is_empty()
            || supplied_by_role
                .insert(document.role, document.bytes)
                .is_some()
        {
            return Err(FredRightsError::TermsEvidenceMismatch);
        }
    }
    let mut verified = BTreeMap::new();
    for role in FredTermsDocumentRole::ALL {
        let evidence = declared
            .get(&role)
            .ok_or(FredRightsError::TermsEvidenceMismatch)?;
        let bytes = supplied_by_role
            .get(&role)
            .ok_or(FredRightsError::TermsEvidenceMismatch)?;
        let reviewed = reviewed_document_bytes(evidence.representation, bytes)?;
        if evidence.byte_length != reviewed.len()
            || evidence.digest != Sha256Digest::of(reviewed.as_ref())
        {
            return Err(FredRightsError::TermsEvidenceMismatch);
        }
        let mut verified_evidence = evidence.clone();
        verified_evidence.raw_digest = Some(Sha256Digest::of(bytes));
        verified_evidence.raw_byte_length = Some(bytes.len());
        if verified.insert(role, verified_evidence).is_some() {
            return Err(FredRightsError::TermsEvidenceMismatch);
        }
    }
    Ok(verified)
}

fn terms_bundle_digest(
    documents: &BTreeMap<FredTermsDocumentRole, FredTermsDocumentEvidence>,
) -> Result<Sha256Digest, FredRightsError> {
    if documents.len() != FredTermsDocumentRole::ALL.len() {
        return Err(FredRightsError::InvalidTerms);
    }
    let document_count =
        u32::try_from(documents.len()).map_err(|_| FredRightsError::InvalidTerms)?;
    let mut hasher = Sha256::new();
    hasher.update(TERMS_BUNDLE_DOMAIN);
    hasher.update(document_count.to_be_bytes());
    for role in FredTermsDocumentRole::ALL {
        let document = documents.get(&role).ok_or(FredRightsError::InvalidTerms)?;
        if document.role != role
            || document.representation != expected_representation(role)
            || document.url != role.canonical_url()
        {
            return Err(FredRightsError::InvalidTerms);
        }
        let url_length =
            u32::try_from(document.url.len()).map_err(|_| FredRightsError::InvalidTerms)?;
        let byte_length =
            u64::try_from(document.byte_length).map_err(|_| FredRightsError::InvalidTerms)?;
        hasher.update([role.tag()]);
        hasher.update([document.representation.tag()]);
        hasher.update(url_length.to_be_bytes());
        hasher.update(document.url.as_bytes());
        hasher.update(byte_length.to_be_bytes());
        hasher.update(document.digest.bytes());
    }
    Ok(Sha256Digest(hasher.finalize().into()))
}

fn validate_terms(terms: &FredTermsEvidence) -> Result<(), FredRightsError> {
    if terms.bundle_digest.is_zero()
        || terms.assessed_at >= terms.review_required_by
        || terms.bundle_digest != terms_bundle_digest(&terms.documents)?
    {
        return Err(FredRightsError::InvalidTerms);
    }
    Ok(())
}

fn has_verified_raw_terms_documents(terms: &FredTermsEvidence) -> bool {
    terms.documents.len() == FredTermsDocumentRole::ALL.len()
        && FredTermsDocumentRole::ALL.iter().all(|role| {
            terms.documents.get(role).is_some_and(|document| {
                document.raw_digest.is_some_and(|digest| !digest.is_zero())
                    && document
                        .raw_byte_length
                        .is_some_and(|length| length > 0 && length <= MAX_FRED_TERMS_DOCUMENT_BYTES)
            })
        })
}

fn validate_operations(operations: &[FredOperation]) -> Result<(), FredRightsError> {
    if operations.is_empty()
        || operations.iter().copied().collect::<BTreeSet<_>>().len() != operations.len()
    {
        return Err(FredRightsError::InvalidOperations);
    }
    Ok(())
}

struct ServicePermissionBinding<'a> {
    channel: &'a FredServicePermissionChannel,
    review: &'a FredServicePermissionReview,
    terms_bundle_digest: Sha256Digest,
    document_digest: Sha256Digest,
    byte_length: usize,
}

fn service_permission_binding_digest(
    binding: ServicePermissionBinding<'_>,
) -> Result<Sha256Digest, FredRightsError> {
    let mut hasher = Sha256::new();
    hasher.update(SERVICE_PERMISSION_DOMAIN);
    hasher.update([1]);
    update_bounded_string(&mut hasher, &binding.channel.evidence_url)?;
    update_bounded_string(&mut hasher, &binding.channel.authority_url)?;
    for value in [
        binding.review.reviewer.as_str(),
        binding.review.issuer.as_str(),
        binding.review.application.as_str(),
        binding.review.service.as_str(),
    ] {
        update_bounded_string(&mut hasher, value)?;
    }
    hasher.update(binding.review.reviewed_at.unix_nanos().to_be_bytes());
    hasher.update(
        u32::try_from(binding.review.series.len())
            .map_err(|_| FredRightsError::ServicePermissionMismatch)?
            .to_be_bytes(),
    );
    for series in &binding.review.series {
        update_bounded_string(&mut hasher, series.as_str())?;
    }
    hasher.update(binding.terms_bundle_digest.bytes());
    let operation_count = u32::try_from(binding.review.operations.len())
        .map_err(|_| FredRightsError::ServicePermissionMismatch)?;
    hasher.update(operation_count.to_be_bytes());
    for operation in &binding.review.operations {
        hasher.update([operation_tag(*operation)]);
    }
    hasher.update(
        u32::try_from(binding.review.conditions.len())
            .map_err(|_| FredRightsError::ServicePermissionMismatch)?
            .to_be_bytes(),
    );
    for condition in &binding.review.conditions {
        update_bounded_string(&mut hasher, condition)?;
    }
    hasher.update(binding.document_digest.bytes());
    hasher.update(
        u64::try_from(binding.byte_length)
            .map_err(|_| FredRightsError::ServicePermissionMismatch)?
            .to_be_bytes(),
    );
    hasher.update(binding.review.effective_at.unix_nanos().to_be_bytes());
    match binding.review.expires_at {
        Some(expires_at) => {
            hasher.update([1]);
            hasher.update(expires_at.unix_nanos().to_be_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update(binding.review.revalidate_by.unix_nanos().to_be_bytes());
    Ok(Sha256Digest(hasher.finalize().into()))
}

fn durable_authority_digest(
    policy: &FredRightsPolicy,
    permission: &FredServicePermissionEvidence,
    admitted_operations: &BTreeMap<SourceIdentifier, BTreeSet<FredOperation>>,
) -> Result<Sha256Digest, FredRightsError> {
    let mut hasher = Sha256::new();
    hasher.update(DURABLE_AUTHORITY_DOMAIN);
    hasher.update(policy.artifact.artifact_digest().bytes());
    hasher.update(policy.artifact.terms_evidence().bundle_digest().bytes());
    hasher.update(permission.binding_digest().bytes());
    hasher.update(
        u32::try_from(admitted_operations.len())
            .map_err(|_| FredRightsError::DurableAuthorityUnavailable)?
            .to_be_bytes(),
    );
    for (series, operations) in admitted_operations {
        let grant = policy
            .grants
            .get(series)
            .ok_or(FredRightsError::DurableAuthorityUnavailable)?;
        update_bounded_string(&mut hasher, series.as_str())?;
        update_bounded_string(&mut hasher, grant.owner().as_str())?;
        hasher.update([match grant.rights_basis() {
            FredSeriesRightsBasis::PublicDomain => 1,
            FredSeriesRightsBasis::OwnerPermission => 2,
        }]);
        update_bounded_string(&mut hasher, grant.authorization_url())?;
        update_bounded_string(&mut hasher, grant.authority_url())?;
        hasher.update(grant.authorization_digest().bytes());
        hasher.update(
            u64::try_from(grant.authorization_byte_length())
                .map_err(|_| FredRightsError::DurableAuthorityUnavailable)?
                .to_be_bytes(),
        );
        hasher.update(grant.effective_at().unix_nanos().to_be_bytes());
        hasher.update(grant.expires_at().unix_nanos().to_be_bytes());
        hasher.update(
            u32::try_from(operations.len())
                .map_err(|_| FredRightsError::DurableAuthorityUnavailable)?
                .to_be_bytes(),
        );
        for operation in operations {
            hasher.update([operation_tag(*operation)]);
        }
    }
    Ok(Sha256Digest(hasher.finalize().into()))
}

fn update_bounded_string(hasher: &mut Sha256, value: &str) -> Result<(), FredRightsError> {
    let length =
        u32::try_from(value.len()).map_err(|_| FredRightsError::ServicePermissionMismatch)?;
    hasher.update(length.to_be_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}

fn validate_service_operations(operations: &[FredOperation]) -> Result<(), FredRightsError> {
    validate_operations(operations)?;
    if operations
        .iter()
        .any(|operation| !requires_service_permission(*operation))
    {
        return Err(FredRightsError::InvalidServicePermission);
    }
    Ok(())
}

const fn operation_tag(operation: FredOperation) -> u8 {
    match operation {
        FredOperation::RetrieveEphemeral => 1,
        FredOperation::Persist => 2,
        FredOperation::Cache => 3,
        FredOperation::Archive => 4,
        FredOperation::Redistribute => 5,
        FredOperation::Display => 6,
        FredOperation::Train => 7,
    }
}

const fn requires_service_permission(operation: FredOperation) -> bool {
    matches!(
        operation,
        FredOperation::Persist
            | FredOperation::Cache
            | FredOperation::Archive
            | FredOperation::Train
    )
}

pub(crate) fn validate_exact_series_id(value: &str) -> Result<(), FredRightsError> {
    if value.is_empty()
        || value.len() > 120
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(FredRightsError::InvalidSeries);
    }
    Ok(())
}

fn validate_exact_rights_owner(value: &str) -> Result<(), FredRightsError> {
    if value.is_empty()
        || value.len() > 256
        || value
            .bytes()
            .any(|byte| !byte.is_ascii_alphanumeric() && !matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(FredRightsError::InvalidRightsOwner);
    }
    Ok(())
}

fn validate_https_url(value: &str) -> Result<(), FredRightsError> {
    let url = Url::parse(value).map_err(|_| FredRightsError::InvalidUrl)?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || url.username() != ""
        || url.password().is_some()
    {
        return Err(FredRightsError::InvalidUrl);
    }
    Ok(())
}

fn validate_st_louis_fed_authority_url(value: &str) -> Result<(), FredRightsError> {
    validate_https_url(value)?;
    let url = Url::parse(value).map_err(|_| FredRightsError::InvalidUrl)?;
    let host = url.host_str().ok_or(FredRightsError::InvalidUrl)?;
    if host != "stlouisfed.org" && !host.ends_with(".stlouisfed.org") {
        return Err(FredRightsError::InvalidServicePermission);
    }
    Ok(())
}
