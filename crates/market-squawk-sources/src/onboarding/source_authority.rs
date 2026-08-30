//! Hidden source, endpoint, and budget authorities for SEC filing dependency retrieval.

use std::fmt::Write as _;
use std::num::{NonZeroU16, NonZeroU32, NonZeroU64};

use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, MetadataRevision,
    RevisionBoundPayloadEvidence, SourceId, SourceIdentifier,
};
use sha2::{Digest as _, Sha256};
use thiserror::Error;
use url::Url;

use crate::{
    ApiEndpointRule, BackoffPolicy, BudgetScope, EndpointPolicy, HttpRequestBounds, PathScope,
    ProviderBudgetPolicy,
};

const SECOND_NANOS: u64 = 1_000_000_000;
const MINUTE_NANOS: u64 = 60 * SECOND_NANOS;
const MEBIBYTE: u64 = 1024 * 1024;
const DESCRIPTOR_EVIDENCE_DOMAIN: &[u8] =
    b"market-squawk/sec-filing-source-authority-descriptor/v1";
const ACTIVATION_EVIDENCE_DOMAIN: &[u8] =
    b"market-squawk/sec-filing-source-authority-activation/v1";
const TAXONOMY_FILE_SUFFIXES: &[&str] = &[".xsd", ".xml"];

/// Stable SEC onboarding surface. Dependency authorities are intentionally not onboarding
/// surfaces.
pub const SEC_EDGAR_PROFILE_ID: &str = "sec.edgar-public";
/// Canonical SEC source identity used by capture, publication, and runtime registration.
pub const SEC_EDGAR_SOURCE_ID: &str = "sec-edgar";
/// Aggregate request-budget scope shared by every SEC-owned EDGAR host.
pub const SEC_EDGAR_RATE_SCOPE: &str = "us-sec-edgar";

/// Canonical FASB taxonomy component source identity.
pub const FASB_XBRL_TAXONOMY_SOURCE_ID: &str = "us-fasb-xbrl-taxonomy";
/// Independent request-budget scope for FASB taxonomy artifacts.
pub const FASB_XBRL_TAXONOMY_RATE_SCOPE: &str = "us-fasb-xbrl-taxonomy";

/// Canonical legacy XBRL US taxonomy component source identity.
pub const XBRL_US_LEGACY_TAXONOMY_SOURCE_ID: &str = "xbrl-us-legacy-taxonomy";
/// Independent request-budget scope for legacy XBRL US taxonomy artifacts.
pub const XBRL_US_LEGACY_TAXONOMY_RATE_SCOPE: &str = "xbrl-us-legacy-taxonomy";

/// Canonical XBRL International standards component source identity.
pub const XBRL_INTERNATIONAL_STANDARDS_SOURCE_ID: &str = "xbrl-international-taxonomy-standards";
/// Independent request-budget scope for XBRL International standards artifacts.
pub const XBRL_INTERNATIONAL_STANDARDS_RATE_SCOPE: &str = "xbrl-international-taxonomy-standards";

/// Canonical W3C XML Schema component source identity.
pub const W3C_XML_SCHEMA_STANDARDS_SOURCE_ID: &str = "w3c-xml-schema-standards";
/// Independent request-budget scope for W3C XML Schema artifacts.
pub const W3C_XML_SCHEMA_STANDARDS_RATE_SCOPE: &str = "w3c-xml-schema-standards";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EndpointAuthority {
    base_url: &'static str,
    path_scope: PathScope,
}

const SEC_EDGAR_LOGICAL_HOSTS: &[&str] = &["data.sec.gov", "www.sec.gov", "xbrl.sec.gov"];
const FASB_XBRL_TAXONOMY_LOGICAL_HOSTS: &[&str] = &["fasb.org", "xbrl.fasb.org"];
const XBRL_US_LEGACY_TAXONOMY_LOGICAL_HOSTS: &[&str] = &["xbrl.us", "taxonomies.xbrl.us"];
const XBRL_INTERNATIONAL_STANDARDS_LOGICAL_HOSTS: &[&str] = &["www.xbrl.org"];
const W3C_XML_SCHEMA_STANDARDS_LOGICAL_HOSTS: &[&str] = &["www.w3.org"];

const SEC_EDGAR_ENDPOINTS: &[EndpointAuthority] = &[
    EndpointAuthority {
        base_url: "https://data.sec.gov/submissions",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://data.sec.gov/api/xbrl",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.sec.gov/Archives/edgar",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.sec.gov/files",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/",
        path_scope: PathScope::Descendants,
    },
];

const SEC_EDGAR_TAXONOMY_ENDPOINTS: &[EndpointAuthority] = &[
    EndpointAuthority {
        base_url: "https://www.sec.gov/Archives/edgar/data",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/dei",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/country",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/currency",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/exch",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/invest",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/naics",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/rr",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/cef",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/ecd",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/oef",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/vip",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/rxp",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/sic",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/spac",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/srt",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://xbrl.sec.gov/stpr",
        path_scope: PathScope::Descendants,
    },
];

const FASB_XBRL_TAXONOMY_ENDPOINTS: &[EndpointAuthority] = &[EndpointAuthority {
    base_url: "https://xbrl.fasb.org/us-gaap",
    path_scope: PathScope::Descendants,
}];

const XBRL_US_LEGACY_TAXONOMY_ENDPOINTS: &[EndpointAuthority] = &[EndpointAuthority {
    base_url: "https://taxonomies.xbrl.us/us-gaap",
    path_scope: PathScope::Descendants,
}];

const XBRL_INTERNATIONAL_STANDARDS_ENDPOINTS: &[EndpointAuthority] = &[
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2003",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2005",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2006",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2008",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2013",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2014",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2016",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2017",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2021",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/2023",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/lrr",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.xbrl.org/dtr",
        path_scope: PathScope::Descendants,
    },
];

const W3C_XML_SCHEMA_STANDARDS_ENDPOINTS: &[EndpointAuthority] = &[
    EndpointAuthority {
        base_url: "https://www.w3.org/1999",
        path_scope: PathScope::Descendants,
    },
    EndpointAuthority {
        base_url: "https://www.w3.org/2001",
        path_scope: PathScope::Descendants,
    },
];

/// Request identity that must be selected with the same authority as the endpoint and budget.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FilingTaxonomyRequestHeaderClass {
    /// SEC-owned requests require the application-owned SEC identifying contact policy.
    SecIdentifyingContact,
    /// External taxonomy publishers receive product identification without SEC contact headers.
    ProductOnlyNoSecContact,
}

impl FilingTaxonomyRequestHeaderClass {
    const fn evidence_label(self) -> &'static str {
        match self {
            Self::SecIdentifyingContact => "sec-identifying-contact",
            Self::ProductOnlyNoSecContact => "product-only-no-sec-contact",
        }
    }
}

/// Immutable hidden component authority for one SEC filing or taxonomy publisher.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilingTaxonomySourceAuthority {
    source_id: &'static str,
    rate_scope: &'static str,
    rate_policy_id: &'static str,
    logical_hosts: &'static [&'static str],
    endpoints: &'static [EndpointAuthority],
    taxonomy_endpoints: &'static [EndpointAuthority],
    request_header_class: FilingTaxonomyRequestHeaderClass,
    requests_per_second: u32,
    max_concurrent: u16,
    max_response_bytes: u64,
    total_timeout_nanos: u64,
}

impl FilingTaxonomySourceAuthority {
    /// Returns the canonical source identity used in component capture and durable provenance.
    pub const fn source_id(self) -> &'static str {
        self.source_id
    }

    /// Constructs the typed canonical source identity for exact metadata registration.
    pub fn canonical_source_id(self) -> Result<SourceId, FilingTaxonomyAuthorityContractError> {
        SourceId::try_from(self.source_id)
            .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidDescriptorIdentity)
    }

    /// Returns the independent provider budget identity. It is not the source identity.
    pub const fn rate_scope(self) -> &'static str {
        self.rate_scope
    }

    /// Returns the immutable rate-policy identity bound into descriptor evidence.
    pub const fn rate_policy_id(self) -> &'static str {
        self.rate_policy_id
    }

    /// Returns the request-header authority that must be used for this publisher.
    pub const fn request_header_class(self) -> FilingTaxonomyRequestHeaderClass {
        self.request_header_class
    }

    /// Builds the bounded HTTPS-only endpoint policy for this exact publisher.
    pub fn endpoint_policy(self) -> Result<EndpointPolicy, FilingTaxonomyAuthorityContractError> {
        self.policy_for_endpoints(self.endpoints)
    }

    /// Builds the code-owned application ceiling for this exact rate scope.
    pub fn budget_policy(
        self,
    ) -> Result<ProviderBudgetPolicy, FilingTaxonomyAuthorityContractError> {
        let scope = BudgetScope::new(
            SourceIdentifier::try_from(self.rate_scope)
                .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidDescriptorIdentity)?,
        );
        let backoff =
            BackoffPolicy::try_new(nonzero_u64(SECOND_NANOS)?, nonzero_u64(MINUTE_NANOS)?, 0)
                .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidBudgetPolicy)?;
        ProviderBudgetPolicy::try_new(
            scope,
            NonZeroU32::new(self.requests_per_second)
                .ok_or(FilingTaxonomyAuthorityContractError::InvalidBudgetPolicy)?,
            nonzero_u64(SECOND_NANOS)?,
            NonZeroU16::new(self.max_concurrent)
                .ok_or(FilingTaxonomyAuthorityContractError::InvalidBudgetPolicy)?,
            backoff,
        )
        .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidBudgetPolicy)
    }

    /// Returns deterministic evidence for the complete code-owned authority descriptor.
    pub fn descriptor_evidence_digest(
        self,
    ) -> Result<EvidenceDigest, FilingTaxonomyAuthorityContractError> {
        let endpoint_policy = serde_json::to_vec(&self.endpoint_policy()?)
            .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidDescriptorEvidence)?;
        let taxonomy_policy =
            serde_json::to_vec(&self.policy_for_endpoints(self.taxonomy_endpoints)?)
                .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidDescriptorEvidence)?;
        let budget_policy = serde_json::to_vec(&self.budget_policy()?)
            .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidDescriptorEvidence)?;
        let mut hasher = Sha256::new();
        hash_field(&mut hasher, b"domain", DESCRIPTOR_EVIDENCE_DOMAIN);
        hash_field(&mut hasher, b"source-id", self.source_id.as_bytes());
        hash_field(
            &mut hasher,
            b"rate-policy-id",
            self.rate_policy_id.as_bytes(),
        );
        hash_field(
            &mut hasher,
            b"request-header-class",
            self.request_header_class.evidence_label().as_bytes(),
        );
        for logical_host in self.logical_hosts {
            hash_field(
                &mut hasher,
                b"logical-publisher-host",
                logical_host.as_bytes(),
            );
        }
        hash_field(&mut hasher, b"endpoint-policy", &endpoint_policy);
        hash_field(&mut hasher, b"taxonomy-route-policy", &taxonomy_policy);
        hash_field(&mut hasher, b"budget-policy", &budget_policy);
        for suffix in TAXONOMY_FILE_SUFFIXES {
            hash_field(&mut hasher, b"taxonomy-file-suffix", suffix.as_bytes());
        }
        Ok(EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            hasher.finalize().into(),
        ))
    }

    /// Returns the deterministic metadata revision derived from the exact descriptor evidence.
    pub fn metadata_revision(
        self,
    ) -> Result<MetadataRevision, FilingTaxonomyAuthorityContractError> {
        metadata_revision_from_digest("authority-", self.descriptor_evidence_digest()?)
    }

    /// Atomically binds the code-owned metadata revision to its exact descriptor digest.
    pub fn revision_evidence(
        self,
    ) -> Result<RevisionBoundPayloadEvidence, FilingTaxonomyAuthorityContractError> {
        let digest = self.descriptor_evidence_digest()?;
        Ok(RevisionBoundPayloadEvidence::new(
            metadata_revision_from_digest("authority-", digest)?,
            ExactPayloadEvidence::from_content_digest(digest),
        ))
    }

    /// Binds one activation receipt to the complete code-owned descriptor for runtime metadata.
    pub fn activation_revision_evidence(
        self,
        activation_evidence: EvidenceDigest,
    ) -> Result<RevisionBoundPayloadEvidence, FilingTaxonomyAuthorityContractError> {
        let descriptor_evidence = self.descriptor_evidence_digest()?;
        let mut hasher = Sha256::new();
        hash_field(&mut hasher, b"domain", ACTIVATION_EVIDENCE_DOMAIN);
        hash_digest(&mut hasher, b"activation-evidence", activation_evidence);
        hash_digest(&mut hasher, b"authority-descriptor", descriptor_evidence);
        let combined = EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Ok(RevisionBoundPayloadEvidence::new(
            metadata_revision_from_digest("activation-authority-", combined)?,
            ExactPayloadEvidence::from_content_digest(combined),
        ))
    }

    fn policy_for_endpoints(
        self,
        endpoints: &[EndpointAuthority],
    ) -> Result<EndpointPolicy, FilingTaxonomyAuthorityContractError> {
        let rules = endpoints
            .iter()
            .map(|authority| {
                ApiEndpointRule::try_new(authority.base_url, authority.path_scope, Vec::new(), 1, 1)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidEndpointPolicy)?;
        let request_bounds = HttpRequestBounds::try_new(
            nonzero_u64(5 * SECOND_NANOS)?,
            nonzero_u64(30 * SECOND_NANOS)?,
            nonzero_u64(self.total_timeout_nanos)?,
            0,
            nonzero_u64(self.max_response_bytes)?,
        )
        .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidEndpointPolicy)?;
        EndpointPolicy::try_from_api_rules(rules, request_bounds)
            .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidEndpointPolicy)
    }

    fn authorize_taxonomy_physical_locator(
        self,
        physical_locator: &str,
        physical: &Url,
    ) -> Result<(), FilingTaxonomyAuthorityLookupError> {
        if !TAXONOMY_FILE_SUFFIXES
            .iter()
            .any(|suffix| physical.path().ends_with(suffix))
        {
            return Err(FilingTaxonomyAuthorityLookupError::UnsupportedPhysicalLocator);
        }
        self.policy_for_endpoints(self.taxonomy_endpoints)
            .map_err(|_| FilingTaxonomyAuthorityLookupError::InvalidAuthorityContract)?
            .authorize_request(physical_locator)
            .map_err(|_| FilingTaxonomyAuthorityLookupError::UnsupportedPhysicalLocator)?;
        Ok(())
    }

    fn owns_logical_host(self, host: &str) -> bool {
        self.logical_hosts.contains(&host)
    }

    fn owns_taxonomy_physical_host(
        self,
        host: &str,
    ) -> Result<bool, FilingTaxonomyAuthorityContractError> {
        for endpoint in self.taxonomy_endpoints {
            let base = Url::parse(endpoint.base_url)
                .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidEndpointPolicy)?;
            if base.host_str() == Some(host) {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// SEC-owned EDGAR authority. Its three origins share one aggregate SEC budget.
pub const SEC_EDGAR_AUTHORITY: FilingTaxonomySourceAuthority = FilingTaxonomySourceAuthority {
    source_id: SEC_EDGAR_SOURCE_ID,
    rate_scope: SEC_EDGAR_RATE_SCOPE,
    rate_policy_id: "sec.edgar-public.aggregate-rate-policy.v1",
    logical_hosts: SEC_EDGAR_LOGICAL_HOSTS,
    endpoints: SEC_EDGAR_ENDPOINTS,
    taxonomy_endpoints: SEC_EDGAR_TAXONOMY_ENDPOINTS,
    request_header_class: FilingTaxonomyRequestHeaderClass::SecIdentifyingContact,
    requests_per_second: 2,
    max_concurrent: 1,
    max_response_bytes: 1024 * MEBIBYTE,
    total_timeout_nanos: 15 * MINUTE_NANOS,
};

/// FASB-published modern US-GAAP taxonomy component authority.
pub const FASB_XBRL_TAXONOMY_AUTHORITY: FilingTaxonomySourceAuthority =
    FilingTaxonomySourceAuthority {
        source_id: FASB_XBRL_TAXONOMY_SOURCE_ID,
        rate_scope: FASB_XBRL_TAXONOMY_RATE_SCOPE,
        rate_policy_id: "fasb.xbrl-taxonomy-public.rate-policy.v1",
        logical_hosts: FASB_XBRL_TAXONOMY_LOGICAL_HOSTS,
        endpoints: FASB_XBRL_TAXONOMY_ENDPOINTS,
        taxonomy_endpoints: FASB_XBRL_TAXONOMY_ENDPOINTS,
        request_header_class: FilingTaxonomyRequestHeaderClass::ProductOnlyNoSecContact,
        requests_per_second: 1,
        max_concurrent: 1,
        max_response_bytes: 8 * MEBIBYTE,
        total_timeout_nanos: 2 * MINUTE_NANOS,
    };

/// XBRL US-published legacy US-GAAP taxonomy component authority.
pub const XBRL_US_LEGACY_TAXONOMY_AUTHORITY: FilingTaxonomySourceAuthority =
    FilingTaxonomySourceAuthority {
        source_id: XBRL_US_LEGACY_TAXONOMY_SOURCE_ID,
        rate_scope: XBRL_US_LEGACY_TAXONOMY_RATE_SCOPE,
        rate_policy_id: "xbrl-us.legacy-taxonomy-public.rate-policy.v1",
        logical_hosts: XBRL_US_LEGACY_TAXONOMY_LOGICAL_HOSTS,
        endpoints: XBRL_US_LEGACY_TAXONOMY_ENDPOINTS,
        taxonomy_endpoints: XBRL_US_LEGACY_TAXONOMY_ENDPOINTS,
        request_header_class: FilingTaxonomyRequestHeaderClass::ProductOnlyNoSecContact,
        requests_per_second: 1,
        max_concurrent: 1,
        max_response_bytes: 8 * MEBIBYTE,
        total_timeout_nanos: 2 * MINUTE_NANOS,
    };

/// XBRL International standards component authority.
pub const XBRL_INTERNATIONAL_STANDARDS_AUTHORITY: FilingTaxonomySourceAuthority =
    FilingTaxonomySourceAuthority {
        source_id: XBRL_INTERNATIONAL_STANDARDS_SOURCE_ID,
        rate_scope: XBRL_INTERNATIONAL_STANDARDS_RATE_SCOPE,
        rate_policy_id: "xbrl-international.taxonomy-standards-public.rate-policy.v1",
        logical_hosts: XBRL_INTERNATIONAL_STANDARDS_LOGICAL_HOSTS,
        endpoints: XBRL_INTERNATIONAL_STANDARDS_ENDPOINTS,
        taxonomy_endpoints: XBRL_INTERNATIONAL_STANDARDS_ENDPOINTS,
        request_header_class: FilingTaxonomyRequestHeaderClass::ProductOnlyNoSecContact,
        requests_per_second: 1,
        max_concurrent: 1,
        max_response_bytes: 8 * MEBIBYTE,
        total_timeout_nanos: 2 * MINUTE_NANOS,
    };

/// W3C XML Schema standards component authority.
pub const W3C_XML_SCHEMA_STANDARDS_AUTHORITY: FilingTaxonomySourceAuthority =
    FilingTaxonomySourceAuthority {
        source_id: W3C_XML_SCHEMA_STANDARDS_SOURCE_ID,
        rate_scope: W3C_XML_SCHEMA_STANDARDS_RATE_SCOPE,
        rate_policy_id: "w3c.xml-schema-standards-public.rate-policy.v1",
        logical_hosts: W3C_XML_SCHEMA_STANDARDS_LOGICAL_HOSTS,
        endpoints: W3C_XML_SCHEMA_STANDARDS_ENDPOINTS,
        taxonomy_endpoints: W3C_XML_SCHEMA_STANDARDS_ENDPOINTS,
        request_header_class: FilingTaxonomyRequestHeaderClass::ProductOnlyNoSecContact,
        requests_per_second: 1,
        max_concurrent: 1,
        max_response_bytes: 8 * MEBIBYTE,
        total_timeout_nanos: 2 * MINUTE_NANOS,
    };

/// Complete bounded set of hidden code-owned filing component authorities.
pub const FILING_TAXONOMY_SOURCE_AUTHORITIES: [FilingTaxonomySourceAuthority; 5] = [
    SEC_EDGAR_AUTHORITY,
    FASB_XBRL_TAXONOMY_AUTHORITY,
    XBRL_US_LEGACY_TAXONOMY_AUTHORITY,
    XBRL_INTERNATIONAL_STANDARDS_AUTHORITY,
    W3C_XML_SCHEMA_STANDARDS_AUTHORITY,
];

/// One exact logical taxonomy reference and the physical HTTPS artifact chosen for retrieval.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FilingTaxonomyLocator<'a> {
    logical_locator: &'a str,
    physical_locator: &'a str,
}

impl<'a> FilingTaxonomyLocator<'a> {
    /// Retains the exact logical and physical locators without normalization or substitution.
    pub const fn new(logical_locator: &'a str, physical_locator: &'a str) -> Self {
        Self {
            logical_locator,
            physical_locator,
        }
    }

    /// Returns the exact logical reference declared by the filing or imported taxonomy.
    pub const fn logical_locator(self) -> &'a str {
        self.logical_locator
    }

    /// Returns the exact physical HTTPS artifact selected for retrieval.
    pub const fn physical_locator(self) -> &'a str {
        self.physical_locator
    }
}

/// Fail-closed result of resolving one logical/physical locator pair to one publisher authority.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResolvedFilingTaxonomyAuthority<'a> {
    locator: FilingTaxonomyLocator<'a>,
    authority: FilingTaxonomySourceAuthority,
}

impl<'a> ResolvedFilingTaxonomyAuthority<'a> {
    /// Returns the exact unmodified logical/physical locator pair.
    pub const fn locator(self) -> FilingTaxonomyLocator<'a> {
        self.locator
    }

    /// Returns the independent source, endpoint, budget, and header authority for this pair.
    pub const fn authority(self) -> FilingTaxonomySourceAuthority {
        self.authority
    }
}

/// Routes one physical HTTPS artifact to its exact code-owned publisher authority.
///
/// # Errors
///
/// Rejects malformed, ambiguous, insecure, query-bearing, or unsupported physical locators.
pub fn route_filing_taxonomy_physical_locator(
    physical_locator: &str,
) -> Result<FilingTaxonomySourceAuthority, FilingTaxonomyAuthorityLookupError> {
    let physical = validate_physical_locator(physical_locator)?;
    let authority = authority_for_physical_host(physical.host_str())?;
    authority.authorize_taxonomy_physical_locator(physical_locator, &physical)?;
    Ok(authority)
}

/// Resolves one exact logical/physical taxonomy pair to its independent source authority.
///
/// Official legacy HTTP logical references are retained. The selected physical locator must be a
/// query-free HTTPS target admitted by that same publisher authority.
///
/// # Errors
///
/// Rejects malformed or unsupported logical references, unsafe physical targets, and any
/// logical-to-physical publisher substitution.
pub fn resolve_filing_taxonomy_authority(
    locator: FilingTaxonomyLocator<'_>,
) -> Result<ResolvedFilingTaxonomyAuthority<'_>, FilingTaxonomyAuthorityLookupError> {
    let logical = validate_logical_locator(locator.logical_locator)?;
    let logical_authority = authority_for_logical_host(logical.host_str())?;
    let physical_authority = route_filing_taxonomy_physical_locator(locator.physical_locator)?;
    if logical_authority != physical_authority {
        return Err(FilingTaxonomyAuthorityLookupError::LogicalPhysicalAuthorityMismatch);
    }
    Ok(ResolvedFilingTaxonomyAuthority {
        locator,
        authority: logical_authority,
    })
}

/// Failure to construct a code-owned component authority.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FilingTaxonomyAuthorityContractError {
    /// A code-owned source, budget, or revision identity is invalid.
    #[error("code-owned filing taxonomy authority identity is invalid")]
    InvalidDescriptorIdentity,
    /// A code-owned authority could not serialize its exact policies for descriptor evidence.
    #[error("code-owned filing taxonomy authority evidence is invalid")]
    InvalidDescriptorEvidence,
    /// A code-owned endpoint or request-bound policy is invalid.
    #[error("code-owned filing taxonomy endpoint policy is invalid")]
    InvalidEndpointPolicy,
    /// A code-owned request-budget policy is invalid.
    #[error("code-owned filing taxonomy budget policy is invalid")]
    InvalidBudgetPolicy,
}

/// Failure to preserve an exact, supported taxonomy publisher mapping.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum FilingTaxonomyAuthorityLookupError {
    /// The logical locator is malformed or names no code-owned publisher authority.
    #[error("logical taxonomy locator is unsupported")]
    UnsupportedLogicalLocator,
    /// The physical locator is malformed, unsafe, or names no code-owned publisher authority.
    #[error("physical taxonomy locator is unsupported")]
    UnsupportedPhysicalLocator,
    /// The physical locator is not admitted by the logical publisher's authority.
    #[error("logical and physical taxonomy authorities do not match")]
    LogicalPhysicalAuthorityMismatch,
    /// A code-owned authority could not reconstruct its own bounded policy.
    #[error("code-owned taxonomy authority contract is invalid")]
    InvalidAuthorityContract,
}

fn validate_logical_locator(
    logical_locator: &str,
) -> Result<Url, FilingTaxonomyAuthorityLookupError> {
    let logical = Url::parse(logical_locator)
        .map_err(|_| FilingTaxonomyAuthorityLookupError::UnsupportedLogicalLocator)?;
    if !matches!(logical.scheme(), "http" | "https")
        || !logical.username().is_empty()
        || logical.password().is_some()
        || has_explicit_port(logical_locator)
        || logical.query().is_some()
        || logical.fragment().is_some()
    {
        return Err(FilingTaxonomyAuthorityLookupError::UnsupportedLogicalLocator);
    }
    Ok(logical)
}

fn validate_physical_locator(
    physical_locator: &str,
) -> Result<Url, FilingTaxonomyAuthorityLookupError> {
    let physical = Url::parse(physical_locator)
        .map_err(|_| FilingTaxonomyAuthorityLookupError::UnsupportedPhysicalLocator)?;
    if physical.scheme() != "https"
        || !physical.username().is_empty()
        || physical.password().is_some()
        || has_explicit_port(physical_locator)
        || physical.query().is_some()
        || physical.fragment().is_some()
    {
        return Err(FilingTaxonomyAuthorityLookupError::UnsupportedPhysicalLocator);
    }
    Ok(physical)
}

fn authority_for_logical_host(
    host: Option<&str>,
) -> Result<FilingTaxonomySourceAuthority, FilingTaxonomyAuthorityLookupError> {
    let host = host.ok_or(FilingTaxonomyAuthorityLookupError::UnsupportedLogicalLocator)?;
    unique_authority_matching(
        |authority| Ok(authority.owns_logical_host(host)),
        FilingTaxonomyAuthorityLookupError::UnsupportedLogicalLocator,
    )
}

fn authority_for_physical_host(
    host: Option<&str>,
) -> Result<FilingTaxonomySourceAuthority, FilingTaxonomyAuthorityLookupError> {
    let host = host.ok_or(FilingTaxonomyAuthorityLookupError::UnsupportedPhysicalLocator)?;
    unique_authority_matching(
        |authority| {
            authority
                .owns_taxonomy_physical_host(host)
                .map_err(|_| FilingTaxonomyAuthorityLookupError::InvalidAuthorityContract)
        },
        FilingTaxonomyAuthorityLookupError::UnsupportedPhysicalLocator,
    )
}

fn unique_authority_matching(
    mut predicate: impl FnMut(
        FilingTaxonomySourceAuthority,
    ) -> Result<bool, FilingTaxonomyAuthorityLookupError>,
    unsupported: FilingTaxonomyAuthorityLookupError,
) -> Result<FilingTaxonomySourceAuthority, FilingTaxonomyAuthorityLookupError> {
    let mut selected = None;
    for authority in FILING_TAXONOMY_SOURCE_AUTHORITIES {
        if predicate(authority)? {
            if selected.is_some() {
                return Err(FilingTaxonomyAuthorityLookupError::InvalidAuthorityContract);
            }
            selected = Some(authority);
        }
    }
    selected.ok_or(unsupported)
}

fn has_explicit_port(locator: &str) -> bool {
    locator
        .split_once("://")
        .map(|(_, remainder)| {
            remainder
                .split(['/', '?', '#'])
                .next()
                .is_some_and(|authority| authority.contains(':'))
        })
        .unwrap_or(false)
}

fn hash_field(hasher: &mut Sha256, label: &[u8], value: &[u8]) {
    hasher.update((label.len() as u64).to_be_bytes());
    hasher.update(label);
    hasher.update((value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn hash_digest(hasher: &mut Sha256, label: &[u8], digest: EvidenceDigest) {
    let algorithm = match digest.algorithm() {
        DigestAlgorithm::Sha256 => b"sha256".as_slice(),
        DigestAlgorithm::Blake3 => b"blake3".as_slice(),
    };
    hash_field(hasher, b"digest-algorithm", algorithm);
    hash_field(hasher, label, &digest.bytes());
}

fn metadata_revision_from_digest(
    prefix: &str,
    digest: EvidenceDigest,
) -> Result<MetadataRevision, FilingTaxonomyAuthorityContractError> {
    let mut revision = String::with_capacity(prefix.len().saturating_add(64));
    revision.push_str(prefix);
    for byte in digest.bytes() {
        write!(&mut revision, "{byte:02x}")
            .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidDescriptorIdentity)?;
    }
    let revision = SourceIdentifier::try_from(revision)
        .map_err(|_| FilingTaxonomyAuthorityContractError::InvalidDescriptorIdentity)?;
    Ok(MetadataRevision::new(revision))
}

fn nonzero_u64(value: u64) -> Result<NonZeroU64, FilingTaxonomyAuthorityContractError> {
    NonZeroU64::new(value).ok_or(FilingTaxonomyAuthorityContractError::InvalidDescriptorIdentity)
}
