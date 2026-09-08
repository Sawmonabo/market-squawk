//! Exact non-secret BEA source/configuration binding.

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, MetadataRevision, SourceId};
use sha2::{Digest as _, Sha256};
use thiserror::Error;

/// Invalid source, credential-generation, or quota binding evidence.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum BeaSourceBindingError {
    /// A required SHA-256 commitment was absent or invalid.
    #[error("invalid BEA source binding")]
    InvalidBinding,
}

/// Exact non-secret coordinates shared by doctor and canonical-candidate construction.
///
/// This is a configuration identity, not activation, restart, publication, or query authority.
/// It contains only commitments to the source contract, configured datasets, protected credential
/// generation, and shared quota declaration; the BEA `UserID` is never retained here. Common
/// application rights and activation leases remain composition authority and are not duplicated.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BeaSourceBinding {
    source_id: SourceId,
    metadata_revision: MetadataRevision,
    config_digest: EvidenceDigest,
    credential_generation_digest: EvidenceDigest,
    quota_declaration_digest: EvidenceDigest,
    binding_digest: EvidenceDigest,
}

impl BeaSourceBinding {
    /// Constructs a complete binding without retaining the BEA `UserID`.
    pub fn try_new(
        source_id: SourceId,
        metadata_revision: MetadataRevision,
        config_digest: EvidenceDigest,
        credential_generation_digest: EvidenceDigest,
        quota_declaration_digest: EvidenceDigest,
    ) -> Result<Self, BeaSourceBindingError> {
        if [
            config_digest,
            credential_generation_digest,
            quota_declaration_digest,
        ]
        .iter()
        .any(|digest| !valid_digest(*digest))
        {
            return Err(BeaSourceBindingError::InvalidBinding);
        }
        let mut hasher = Sha256::new();
        hasher.update(b"market-squawk/bea-source-binding/v2");
        hash_text(&mut hasher, source_id.as_str());
        hash_text(
            &mut hasher,
            metadata_revision.as_source_identifier().as_str(),
        );
        for digest in [
            config_digest,
            credential_generation_digest,
            quota_declaration_digest,
        ] {
            hasher.update(digest.bytes());
        }
        let binding_digest = EvidenceDigest::new(DigestAlgorithm::Sha256, hasher.finalize().into());
        Ok(Self {
            source_id,
            metadata_revision,
            config_digest,
            credential_generation_digest,
            quota_declaration_digest,
            binding_digest,
        })
    }

    /// Returns the registered source identity.
    pub const fn source_id(&self) -> &SourceId {
        &self.source_id
    }

    /// Returns the exact registered metadata revision.
    pub const fn metadata_revision(&self) -> &MetadataRevision {
        &self.metadata_revision
    }

    /// Returns the configured dataset-contract commitment.
    pub const fn config_digest(&self) -> EvidenceDigest {
        self.config_digest
    }

    /// Returns the protected credential-generation commitment, never the credential.
    pub const fn credential_generation_digest(&self) -> EvidenceDigest {
        self.credential_generation_digest
    }

    /// Returns the complete request/byte/error declaration commitment.
    pub const fn quota_declaration_digest(&self) -> EvidenceDigest {
        self.quota_declaration_digest
    }

    /// Returns the complete source binding commitment.
    pub const fn binding_digest(&self) -> EvidenceDigest {
        self.binding_digest
    }
}

fn valid_digest(digest: EvidenceDigest) -> bool {
    digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes() != [0; 32]
}

fn hash_text(hasher: &mut Sha256, value: &str) {
    hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(value.as_bytes());
}
