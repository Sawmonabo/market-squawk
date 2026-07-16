//! Shared cryptographic digest algorithms for payload and canonical-state evidence.

use serde::{Deserialize, Serialize};

/// Cryptographic algorithm used for a retained 256-bit digest.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DigestAlgorithm {
    /// SHA-256 digest.
    Sha256,
    /// BLAKE3 digest.
    Blake3,
}
