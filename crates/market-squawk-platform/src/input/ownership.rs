//! Truthful local-ownership evidence issued only by explicit application composition.

use std::fmt;
use std::path::Path;
use std::sync::Arc;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest};
use sha2::{Digest as _, Sha256};

use super::{BoundedInput, FileSystemIdentity, InputFileError, InputRootInner};

const ROOT_IDENTITY_DOMAIN: &[u8] = b"market-squawk/user-owned-input-root-identity";
const ROOT_IDENTITY_SCHEMA_VERSION: u16 = 1;

/// Path-free SHA-256 identity of one retained local input directory.
///
/// This digest is stable while the same filesystem object remains at the authorized root path.
/// It changes when that root is replaced. It is historical identity evidence, not filesystem or
/// research-use authority by itself.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub struct UserOwnedInputRootIdentityDigest(EvidenceDigest);

/// Non-transferable issuer created only at the explicit local-ownership composition boundary.
pub struct UserOwnedInputAuthority {
    root: Arc<InputRootInner>,
}

/// Historical proof that an exact verified manifest was read from one user-owned input root.
///
/// This evidence is deliberately non-cloneable and non-serializable. Callers may persist only its
/// typed, path-free root identity digest and exact manifest content digest through a higher-level
/// audited data-layer contract.
pub struct UserOwnedInputEvidence {
    root_identity_digest: UserOwnedInputRootIdentityDigest,
    manifest_digest: EvidenceDigest,
}

impl super::UserAuthorizedInputRoot {
    /// Opens a user-selected root and separately returns its local-ownership evidence issuer.
    ///
    /// This is the sole composition boundary that creates [`UserOwnedInputAuthority`]. Ordinary
    /// [`Self::open`] and clones of [`Self`] carry read authority only and cannot issue ownership
    /// evidence.
    ///
    /// # Errors
    ///
    /// Returns [`InputFileError`] when the root cannot be opened under the hardened no-follow and
    /// stable-identity contract.
    pub fn open_with_ownership_authority(
        path: impl AsRef<Path>,
    ) -> Result<(Self, UserOwnedInputAuthority), InputFileError> {
        let root = Self::open(path)?;
        let authority = UserOwnedInputAuthority {
            root: Arc::clone(&root.inner),
        };
        Ok((root, authority))
    }
}

impl UserOwnedInputAuthority {
    /// Issues evidence for an exact manifest read from this issuer's retained root.
    ///
    /// The input must have completed both bounded digest passes and final identity revalidation.
    /// A read from another opening of the same directory is rejected: that separate composition
    /// boundary must use its own issuer. The path identity is revalidated immediately before
    /// evidence is released, so root replacement fails closed.
    ///
    /// # Errors
    ///
    /// Returns [`InputFileError::IdentityChanged`] when the bounded input did not originate from
    /// this exact retained root, or when the retained root was replaced. Other path-redacted root
    /// revalidation failures retain their exact [`InputFileError`] classification.
    pub fn issue_manifest_evidence(
        &self,
        manifest: &BoundedInput,
    ) -> Result<UserOwnedInputEvidence, InputFileError> {
        if !Arc::ptr_eq(
            &self.root.ownership_binding,
            &manifest.root_ownership_binding,
        ) {
            return Err(InputFileError::IdentityChanged);
        }
        self.root.validate_root()?;
        let root_identity_digest = self.root.ownership_binding.identity_digest;
        if root_identity_digest != manifest.root_ownership_binding.identity_digest {
            return Err(InputFileError::IdentityChanged);
        }
        Ok(UserOwnedInputEvidence {
            root_identity_digest,
            manifest_digest: manifest.digest(),
        })
    }
}

impl UserOwnedInputEvidence {
    /// Returns the typed, path-free identity digest of the exact verified input root.
    pub const fn root_identity_digest(&self) -> UserOwnedInputRootIdentityDigest {
        self.root_identity_digest
    }

    /// Returns the exact two-pass SHA-256 digest of the verified manifest bytes.
    pub const fn manifest_digest(&self) -> EvidenceDigest {
        self.manifest_digest
    }
}

impl UserOwnedInputRootIdentityDigest {
    /// Returns the algorithm-qualified digest for durable audited binding.
    pub const fn evidence_digest(self) -> EvidenceDigest {
        self.0
    }
}

pub(super) fn root_identity_digest(
    identity: FileSystemIdentity,
) -> UserOwnedInputRootIdentityDigest {
    let mut hasher = Sha256::new();
    hasher.update(ROOT_IDENTITY_DOMAIN);
    hasher.update([0]);
    hasher.update(ROOT_IDENTITY_SCHEMA_VERSION.to_be_bytes());
    hasher.update([1]);
    hasher.update(identity.device.to_be_bytes());
    hasher.update([2]);
    hasher.update(identity.inode.to_be_bytes());
    UserOwnedInputRootIdentityDigest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        hasher.finalize().into(),
    ))
}

impl fmt::Debug for UserOwnedInputRootIdentityDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UserOwnedInputRootIdentityDigest([REDACTED])")
    }
}

impl fmt::Debug for UserOwnedInputAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UserOwnedInputAuthority([RETAINED ROOT AUTHORITY])")
    }
}

impl fmt::Debug for UserOwnedInputEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("UserOwnedInputEvidence([REDACTED])")
    }
}
