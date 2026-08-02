//! Bounded generation envelopes and sealed whole-payload authentication context.

use std::fmt;

use sha2::{Digest as _, Sha256};
use zeroize::{Zeroize as _, Zeroizing};

use super::LocalAuthorityStateStoreError;

pub(super) const MAX_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
pub(super) const DIGEST_BYTES: usize = 32;
const MAGIC: [u8; 8] = *b"MSQAUTH\0";
const CONTEXT_MAGIC: [u8; 8] = *b"MSQCTX\0\0";
const DIGEST_DOMAIN: &[u8] = b"market-squawk-authority-envelope-v3\0";
const VERSION: u16 = 3;
const GENERATION_BYTES: usize = size_of::<u64>();
const HEADER_BYTES: usize = MAGIC.len()
    + size_of::<u16>()
    + GENERATION_BYTES
    + DIGEST_BYTES
    + GENERATION_BYTES
    + DIGEST_BYTES
    + size_of::<u64>()
    + DIGEST_BYTES
    + DIGEST_BYTES;
pub(super) const MAX_ENVELOPE_BYTES: usize = HEADER_BYTES + MAX_PAYLOAD_BYTES;
pub(super) const ZERO_DIGEST: [u8; DIGEST_BYTES] = [0; DIGEST_BYTES];

/// Sealed logical-commit context for whole-payload authentication.
#[derive(Eq, PartialEq)]
pub struct AuthorityCommitContext {
    pub(super) generation: u64,
    pub(super) predecessor: [u8; DIGEST_BYTES],
}

impl AuthorityCommitContext {
    /// Returns fixed canonical bytes suitable for a keyed whole-payload authenticator.
    pub fn authentication_bytes(&self) -> [u8; 48] {
        let mut bytes = [0_u8; 48];
        bytes[..CONTEXT_MAGIC.len()].copy_from_slice(&CONTEXT_MAGIC);
        bytes[CONTEXT_MAGIC.len()..CONTEXT_MAGIC.len() + GENERATION_BYTES]
            .copy_from_slice(&self.generation.to_be_bytes());
        bytes[CONTEXT_MAGIC.len() + GENERATION_BYTES..].copy_from_slice(&self.predecessor);
        bytes
    }
}

impl fmt::Debug for AuthorityCommitContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorityCommitContext([SEALED GENERATION])")
    }
}

/// Verified authority payload and the logical context to which it was committed.
pub struct AuthorityStateSnapshot {
    payload: Vec<u8>,
    context: AuthorityCommitContext,
}

impl AuthorityStateSnapshot {
    /// Returns the verified serialized payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Returns its sealed logical-commit authentication context.
    pub const fn context(&self) -> &AuthorityCommitContext {
        &self.context
    }

    /// Consumes the snapshot and transfers its payload to the caller.
    pub fn into_payload(mut self) -> Vec<u8> {
        std::mem::take(&mut self.payload)
    }
}

impl fmt::Debug for AuthorityStateSnapshot {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthorityStateSnapshot([REDACTED PAYLOAD])")
    }
}

impl Drop for AuthorityStateSnapshot {
    fn drop(&mut self) {
        self.payload.zeroize();
    }
}

pub(super) struct Envelope {
    pub(super) generation: u64,
    pub(super) predecessor: [u8; DIGEST_BYTES],
    pub(super) context: AuthorityCommitContext,
    payload_digest: [u8; DIGEST_BYTES],
    pub(super) payload: Zeroizing<Vec<u8>>,
    pub(super) envelope_digest: [u8; DIGEST_BYTES],
}

impl Envelope {
    pub(super) fn new(
        generation: u64,
        predecessor: [u8; DIGEST_BYTES],
        context: &AuthorityCommitContext,
        payload: Vec<u8>,
    ) -> Result<Self, LocalAuthorityStateStoreError> {
        let payload = Zeroizing::new(payload);
        validate_payload_size(&payload)?;
        validate_copy_shape(generation, predecessor, context)?;
        let payload_digest = Sha256::digest(&payload).into();
        let envelope_digest =
            canonical_digest(generation, &predecessor, context, &payload_digest, &payload)?;
        Ok(Self {
            generation,
            predecessor,
            context: AuthorityCommitContext {
                generation: context.generation,
                predecessor: context.predecessor,
            },
            payload_digest,
            payload,
            envelope_digest,
        })
    }

    pub(super) fn encode(&self) -> Result<Vec<u8>, LocalAuthorityStateStoreError> {
        let payload_len = u64::try_from(self.payload.len())
            .map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
        let capacity = HEADER_BYTES
            .checked_add(self.payload.len())
            .ok_or(LocalAuthorityStateStoreError::Allocation)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.generation.to_be_bytes());
        bytes.extend_from_slice(&self.predecessor);
        bytes.extend_from_slice(&self.context.generation.to_be_bytes());
        bytes.extend_from_slice(&self.context.predecessor);
        bytes.extend_from_slice(&payload_len.to_be_bytes());
        bytes.extend_from_slice(&self.payload_digest);
        bytes.extend_from_slice(&self.envelope_digest);
        bytes.extend_from_slice(&self.payload);
        Ok(bytes)
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, LocalAuthorityStateStoreError> {
        if bytes.len() < HEADER_BYTES || bytes[..MAGIC.len()] != MAGIC {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        let mut cursor = MAGIC.len();
        let version = take_u16(bytes, &mut cursor)?;
        let generation = take_u64(bytes, &mut cursor)?;
        let predecessor = take_digest(bytes, &mut cursor)?;
        let logical_generation = take_u64(bytes, &mut cursor)?;
        let logical_predecessor = take_digest(bytes, &mut cursor)?;
        let payload_len = usize::try_from(take_u64(bytes, &mut cursor)?)
            .map_err(|_| LocalAuthorityStateStoreError::CorruptEnvelope)?;
        let payload_digest = take_digest(bytes, &mut cursor)?;
        let persisted_envelope_digest = take_digest(bytes, &mut cursor)?;
        let expected = cursor
            .checked_add(payload_len)
            .ok_or(LocalAuthorityStateStoreError::CorruptEnvelope)?;
        if version != VERSION || payload_len > MAX_PAYLOAD_BYTES || expected != bytes.len() {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        let context = AuthorityCommitContext {
            generation: logical_generation,
            predecessor: logical_predecessor,
        };
        let payload = &bytes[cursor..];
        if Sha256::digest(payload).as_slice() != payload_digest
            || canonical_digest(generation, &predecessor, &context, &payload_digest, payload)?
                != persisted_envelope_digest
        {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        let envelope = Self::new(generation, predecessor, &context, payload.to_vec())?;
        if envelope.envelope_digest != persisted_envelope_digest {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        Ok(envelope)
    }

    pub(super) fn same_logical_payload(&self, other: &Self) -> bool {
        self.context == other.context && self.payload_digest == other.payload_digest
    }

    pub(super) fn is_first_copy(&self) -> bool {
        self.generation == self.context.generation
    }

    pub(super) fn is_second_copy(&self) -> bool {
        self.context
            .generation
            .checked_add(1)
            .is_some_and(|generation| self.generation == generation)
    }

    pub(super) fn into_snapshot(
        mut self,
    ) -> Result<AuthorityStateSnapshot, LocalAuthorityStateStoreError> {
        if Sha256::digest(&self.payload).as_slice() != self.payload_digest {
            return Err(LocalAuthorityStateStoreError::CorruptEnvelope);
        }
        Ok(AuthorityStateSnapshot {
            payload: std::mem::take(&mut *self.payload),
            context: AuthorityCommitContext {
                generation: self.context.generation,
                predecessor: self.context.predecessor,
            },
        })
    }
}

pub(super) fn next_context(
    head: Option<&Envelope>,
) -> Result<AuthorityCommitContext, LocalAuthorityStateStoreError> {
    match head {
        Some(head) => Ok(AuthorityCommitContext {
            generation: head
                .generation
                .checked_add(1)
                .ok_or(LocalAuthorityStateStoreError::GenerationExhausted)?,
            predecessor: head.envelope_digest,
        }),
        None => Ok(AuthorityCommitContext {
            generation: 1,
            predecessor: ZERO_DIGEST,
        }),
    }
}

pub(super) fn validate_payload_size(payload: &[u8]) -> Result<(), LocalAuthorityStateStoreError> {
    if payload.len() > MAX_PAYLOAD_BYTES {
        Err(LocalAuthorityStateStoreError::PayloadTooLarge {
            bytes: payload.len(),
            maximum: MAX_PAYLOAD_BYTES,
        })
    } else {
        Ok(())
    }
}

fn validate_copy_shape(
    generation: u64,
    predecessor: [u8; DIGEST_BYTES],
    context: &AuthorityCommitContext,
) -> Result<(), LocalAuthorityStateStoreError> {
    let valid_context = context.generation > 0
        && ((context.generation == 1 && context.predecessor == ZERO_DIGEST)
            || (context.generation > 1 && context.predecessor != ZERO_DIGEST));
    let first = generation == context.generation && predecessor == context.predecessor;
    let second = context
        .generation
        .checked_add(1)
        .is_some_and(|second| generation == second && predecessor != ZERO_DIGEST);
    if valid_context && (first || second) {
        Ok(())
    } else {
        Err(LocalAuthorityStateStoreError::GenerationConflict)
    }
}

fn canonical_digest(
    generation: u64,
    predecessor: &[u8; DIGEST_BYTES],
    context: &AuthorityCommitContext,
    payload_digest: &[u8; DIGEST_BYTES],
    payload: &[u8],
) -> Result<[u8; DIGEST_BYTES], LocalAuthorityStateStoreError> {
    let payload_len =
        u64::try_from(payload.len()).map_err(|_| LocalAuthorityStateStoreError::Allocation)?;
    let mut hasher = Sha256::new();
    hasher.update(DIGEST_DOMAIN);
    hasher.update(MAGIC);
    hasher.update(VERSION.to_be_bytes());
    hasher.update(generation.to_be_bytes());
    hasher.update(predecessor);
    hasher.update(context.generation.to_be_bytes());
    hasher.update(context.predecessor);
    hasher.update(payload_len.to_be_bytes());
    hasher.update(payload_digest);
    hasher.update(payload);
    Ok(hasher.finalize().into())
}

fn take_u16(bytes: &[u8], cursor: &mut usize) -> Result<u16, LocalAuthorityStateStoreError> {
    let end = cursor
        .checked_add(size_of::<u16>())
        .ok_or(LocalAuthorityStateStoreError::CorruptEnvelope)?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(LocalAuthorityStateStoreError::CorruptEnvelope)?
        .try_into()
        .map_err(|_| LocalAuthorityStateStoreError::CorruptEnvelope)?;
    *cursor = end;
    Ok(u16::from_be_bytes(value))
}

fn take_u64(bytes: &[u8], cursor: &mut usize) -> Result<u64, LocalAuthorityStateStoreError> {
    let end = cursor
        .checked_add(size_of::<u64>())
        .ok_or(LocalAuthorityStateStoreError::CorruptEnvelope)?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(LocalAuthorityStateStoreError::CorruptEnvelope)?
        .try_into()
        .map_err(|_| LocalAuthorityStateStoreError::CorruptEnvelope)?;
    *cursor = end;
    Ok(u64::from_be_bytes(value))
}

fn take_digest(
    bytes: &[u8],
    cursor: &mut usize,
) -> Result<[u8; DIGEST_BYTES], LocalAuthorityStateStoreError> {
    let end = cursor
        .checked_add(DIGEST_BYTES)
        .ok_or(LocalAuthorityStateStoreError::CorruptEnvelope)?;
    let value = bytes
        .get(*cursor..end)
        .ok_or(LocalAuthorityStateStoreError::CorruptEnvelope)?
        .try_into()
        .map_err(|_| LocalAuthorityStateStoreError::CorruptEnvelope)?;
    *cursor = end;
    Ok(value)
}
