//! Bounded encrypted-vault records and cryptographic operations.

use std::collections::BTreeMap;
use std::fmt;

use argon2::{Algorithm, Argon2, Block, Params, Version};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead as _, KeyInit as _, Payload},
};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor},
};
use zeroize::{Zeroize as _, Zeroizing};

use super::LocalSecretStoreError;
use crate::SecretValue;

pub(super) const VAULT_VERSION: u16 = 2;
pub(super) const MAX_ENTRIES: usize = 24;
const VAULT_MAGIC: &[u8; 8] = b"MSQSECR\0";
const VAULT_VERIFIER: &str = "market-squawk-vault-verifier-v2";
const VERIFIER_TOKEN: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const MAX_CIPHERTEXT_BYTES: usize = 64 * 1024 + 16;
const MAX_CIPHERTEXT_HEX_BYTES: usize = MAX_CIPHERTEXT_BYTES * 2;
const SALT_BYTES: usize = 16;
const NONCE_BYTES: usize = 24;
const KEY_BYTES: usize = 32;
const ARGON_MEMORY_KIB: u32 = 64 * 1024;
const ARGON_ITERATIONS: u32 = 3;
const ARGON_LANES: u32 = 1;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EncryptedSet {
    verifier: EncryptedEntry,
    #[serde(deserialize_with = "deserialize_entries")]
    pub(super) entries: BTreeMap<String, EncryptedEntry>,
}

impl EncryptedSet {
    pub(super) fn empty(unlock: &SecretValue) -> Result<Self, LocalSecretStoreError> {
        let verifier = SecretValue::new(VAULT_VERIFIER.to_owned())
            .map_err(|_| LocalSecretStoreError::CorruptVault)?;
        Ok(Self {
            verifier: encrypt_entry(VERIFIER_TOKEN, &verifier, unlock)?,
            entries: BTreeMap::new(),
        })
    }

    pub(super) fn from_plaintext(
        plaintext: &[(String, SecretValue)],
        unlock: &SecretValue,
    ) -> Result<Self, LocalSecretStoreError> {
        if plaintext.len() > MAX_ENTRIES {
            return Err(LocalSecretStoreError::CapacityExceeded);
        }
        let mut encrypted = Self::empty(unlock)?;
        for (token, value) in plaintext {
            encrypted
                .entries
                .insert(token.clone(), encrypt_entry(token, value, unlock)?);
        }
        Ok(encrypted)
    }

    pub(super) fn validate(&self) -> Result<(), LocalSecretStoreError> {
        if self.entries.len() > MAX_ENTRIES {
            return Err(LocalSecretStoreError::CorruptVault);
        }
        validate_entry(VERIFIER_TOKEN, &self.verifier)?;
        for (token, entry) in &self.entries {
            validate_entry(token, entry)?;
        }
        Ok(())
    }

    pub(super) fn insert(
        &mut self,
        token: String,
        value: &SecretValue,
        unlock: &SecretValue,
    ) -> Result<(), LocalSecretStoreError> {
        let entry = encrypt_entry(&token, value, unlock)?;
        self.entries.insert(token, entry);
        Ok(())
    }

    pub(super) fn decrypt(
        &self,
        token: &str,
        unlock: &SecretValue,
    ) -> Result<SecretValue, LocalSecretStoreError> {
        let entry = self
            .entries
            .get(token)
            .ok_or(LocalSecretStoreError::NotFound)?;
        decrypt_entry(token, entry, unlock)
    }
}

pub(super) fn validate_matching_keys(
    first: &EncryptedSet,
    second: &EncryptedSet,
) -> Result<(), LocalSecretStoreError> {
    if first.entries.keys().ne(second.entries.keys()) {
        return Err(LocalSecretStoreError::CorruptVault);
    }
    Ok(())
}

pub(super) fn decrypt_entries(
    set: &EncryptedSet,
    unlock: &SecretValue,
) -> Result<Vec<(String, SecretValue)>, LocalSecretStoreError> {
    let mut plaintext = Vec::new();
    plaintext
        .try_reserve_exact(set.entries.len())
        .map_err(|_| LocalSecretStoreError::Allocation)?;
    for (token, entry) in &set.entries {
        plaintext.push((token.clone(), decrypt_entry(token, entry, unlock)?));
    }
    Ok(plaintext)
}

pub(super) fn encode_hex(bytes: &[u8]) -> Result<String, LocalSecretStoreError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let capacity = bytes
        .len()
        .checked_mul(2)
        .ok_or(LocalSecretStoreError::Allocation)?;
    let mut encoded = String::new();
    encoded
        .try_reserve_exact(capacity)
        .map_err(|_| LocalSecretStoreError::Allocation)?;
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn deserialize_entries<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, EncryptedEntry>, D::Error>
where
    D: Deserializer<'de>,
{
    struct EntriesVisitor;

    impl<'de> Visitor<'de> for EntriesVisitor {
        type Value = BTreeMap<String, EncryptedEntry>;

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded encrypted-entry map")
        }

        fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut entries = BTreeMap::new();
            while let Some((token, entry)) = access.next_entry::<String, EncryptedEntry>()? {
                if entries.len() == MAX_ENTRIES {
                    return Err(serde::de::Error::custom("encrypted-entry map is too large"));
                }
                if entries.insert(token, entry).is_some() {
                    return Err(serde::de::Error::custom("duplicate encrypted-entry token"));
                }
            }
            Ok(entries)
        }
    }

    deserializer.deserialize_map(EntriesVisitor)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EncryptedEntry {
    memory_kib: u32,
    iterations: u32,
    lanes: u32,
    salt: [u8; SALT_BYTES],
    nonce: [u8; NONCE_BYTES],
    ciphertext: String,
}

#[derive(Deserialize, Serialize)]
#[serde(transparent)]
pub(super) struct VaultAuthenticator(EncryptedEntry);

#[derive(Clone, Copy)]
pub(super) enum VaultAuthenticatorRole {
    StableActive,
    PreparedPrior,
    PreparedCandidate,
    CommittedCandidate,
    CommittedRecovery,
}

impl VaultAuthenticatorRole {
    const fn token(self) -> &'static str {
        match self {
            Self::StableActive => {
                "1111111111111111111111111111111111111111111111111111111111111111"
            }
            Self::PreparedPrior => {
                "2222222222222222222222222222222222222222222222222222222222222222"
            }
            Self::PreparedCandidate => {
                "3333333333333333333333333333333333333333333333333333333333333333"
            }
            Self::CommittedCandidate => {
                "4444444444444444444444444444444444444444444444444444444444444444"
            }
            Self::CommittedRecovery => {
                "5555555555555555555555555555555555555555555555555555555555555555"
            }
        }
    }
}

impl VaultAuthenticator {
    pub(super) fn seal(
        role: VaultAuthenticatorRole,
        digest: &[u8; 32],
        unlock: &SecretValue,
    ) -> Result<Self, LocalSecretStoreError> {
        let value = SecretValue::new(encode_hex(digest)?)
            .map_err(|_| LocalSecretStoreError::CorruptVault)?;
        encrypt_entry(role.token(), &value, unlock).map(Self)
    }

    pub(super) fn validate(
        &self,
        role: VaultAuthenticatorRole,
    ) -> Result<(), LocalSecretStoreError> {
        validate_entry(role.token(), &self.0)
    }

    pub(super) fn authenticate(
        &self,
        role: VaultAuthenticatorRole,
        digest: &[u8; 32],
        unlock: &SecretValue,
    ) -> Result<(), LocalSecretStoreError> {
        let observed = decrypt_entry(role.token(), &self.0, unlock)?;
        if observed.expose_secret() != encode_hex(digest)? {
            return Err(LocalSecretStoreError::AuthenticationFailed);
        }
        Ok(())
    }
}

fn validate_entry(token: &str, entry: &EncryptedEntry) -> Result<(), LocalSecretStoreError> {
    if token.len() != 64
        || !is_lower_hex(token)
        || entry.memory_kib != ARGON_MEMORY_KIB
        || entry.iterations != ARGON_ITERATIONS
        || entry.lanes != ARGON_LANES
        || entry.ciphertext.len() < 32
        || entry.ciphertext.len() > MAX_CIPHERTEXT_HEX_BYTES
        || !entry.ciphertext.len().is_multiple_of(2)
        || !is_lower_hex(&entry.ciphertext)
    {
        return Err(LocalSecretStoreError::CorruptVault);
    }
    Ok(())
}

fn encrypt_entry(
    token: &str,
    value: &SecretValue,
    unlock: &SecretValue,
) -> Result<EncryptedEntry, LocalSecretStoreError> {
    let mut salt = [0_u8; SALT_BYTES];
    let mut nonce = [0_u8; NONCE_BYTES];
    getrandom::fill(&mut salt).map_err(|_| LocalSecretStoreError::RandomUnavailable)?;
    getrandom::fill(&mut nonce).map_err(|_| LocalSecretStoreError::RandomUnavailable)?;
    let mut entry = EncryptedEntry {
        memory_kib: ARGON_MEMORY_KIB,
        iterations: ARGON_ITERATIONS,
        lanes: ARGON_LANES,
        salt,
        nonce,
        ciphertext: String::new(),
    };
    let derived = derive_key(unlock, &entry)?;
    let cipher = XChaCha20Poly1305::new_from_slice(derived.as_ref())
        .map_err(|_| LocalSecretStoreError::CorruptVault)?;
    let aad = entry_aad(token, &entry);
    let nonce = XNonce::try_from(entry.nonce.as_slice())
        .map_err(|_| LocalSecretStoreError::CorruptVault)?;
    let mut ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: value.expose_secret().as_bytes(),
                aad: &aad,
            },
        )
        .map_err(|_| LocalSecretStoreError::AuthenticationFailed)?;
    entry.ciphertext = encode_hex(&ciphertext)?;
    ciphertext.zeroize();
    Ok(entry)
}

fn decrypt_entry(
    token: &str,
    entry: &EncryptedEntry,
    unlock: &SecretValue,
) -> Result<SecretValue, LocalSecretStoreError> {
    let derived = derive_key(unlock, entry)?;
    let cipher = XChaCha20Poly1305::new_from_slice(derived.as_ref())
        .map_err(|_| LocalSecretStoreError::CorruptVault)?;
    let aad = entry_aad(token, entry);
    let nonce = XNonce::try_from(entry.nonce.as_slice())
        .map_err(|_| LocalSecretStoreError::CorruptVault)?;
    let mut ciphertext = decode_hex(&entry.ciphertext)?;
    let decrypted = cipher.decrypt(
        &nonce,
        Payload {
            msg: &ciphertext,
            aad: &aad,
        },
    );
    ciphertext.zeroize();
    let mut plaintext = decrypted.map_err(|_| LocalSecretStoreError::AuthenticationFailed)?;
    let value = match String::from_utf8(plaintext) {
        Ok(value) => value,
        Err(error) => {
            plaintext = error.into_bytes();
            plaintext.zeroize();
            return Err(LocalSecretStoreError::CorruptVault);
        }
    };
    SecretValue::new(value).map_err(|_| LocalSecretStoreError::InvalidSecret)
}

fn derive_key(
    unlock: &SecretValue,
    entry: &EncryptedEntry,
) -> Result<Zeroizing<[u8; KEY_BYTES]>, LocalSecretStoreError> {
    let params = Params::new(
        entry.memory_kib,
        entry.iterations,
        entry.lanes,
        Some(KEY_BYTES),
    )
    .map_err(|_| LocalSecretStoreError::CorruptVault)?;
    let block_count = params.block_count();
    let mut blocks = Vec::new();
    blocks
        .try_reserve_exact(block_count)
        .map_err(|_| LocalSecretStoreError::Allocation)?;
    blocks.resize(block_count, Block::default());
    let mut blocks = Zeroizing::new(blocks);
    let mut derived = Zeroizing::new([0_u8; KEY_BYTES]);
    Argon2::new(Algorithm::Argon2id, Version::V0x13, params)
        .hash_password_into_with_memory(
            unlock.expose_secret().as_bytes(),
            &entry.salt,
            derived.as_mut(),
            blocks.as_mut_slice(),
        )
        .map_err(|_| LocalSecretStoreError::CorruptVault)?;
    Ok(derived)
}

fn entry_aad(token: &str, entry: &EncryptedEntry) -> Vec<u8> {
    let mut aad = Vec::with_capacity(8 + 2 + 64 + 12 + SALT_BYTES);
    aad.extend_from_slice(VAULT_MAGIC);
    aad.extend_from_slice(&VAULT_VERSION.to_be_bytes());
    aad.extend_from_slice(token.as_bytes());
    aad.extend_from_slice(&entry.memory_kib.to_be_bytes());
    aad.extend_from_slice(&entry.iterations.to_be_bytes());
    aad.extend_from_slice(&entry.lanes.to_be_bytes());
    aad.extend_from_slice(&entry.salt);
    aad
}

fn is_lower_hex(value: &str) -> bool {
    value
        .bytes()
        .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

fn decode_hex(value: &str) -> Result<Vec<u8>, LocalSecretStoreError> {
    if !value.len().is_multiple_of(2)
        || value.len() > MAX_CIPHERTEXT_HEX_BYTES
        || !is_lower_hex(value)
    {
        return Err(LocalSecretStoreError::CorruptVault);
    }
    let decoded_len = value.len() / 2;
    let mut decoded = Vec::new();
    decoded
        .try_reserve_exact(decoded_len)
        .map_err(|_| LocalSecretStoreError::Allocation)?;
    for pair in value.as_bytes().chunks_exact(2) {
        let high = decode_nibble(pair[0]).ok_or(LocalSecretStoreError::CorruptVault)?;
        let low = decode_nibble(pair[1]).ok_or(LocalSecretStoreError::CorruptVault)?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

fn decode_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}
