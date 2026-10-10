use sha2::{Digest as _, Sha256};

pub(crate) fn update_tag(digest: &mut Sha256, value: &str) {
    update_bytes(digest, value.as_bytes());
}

pub(crate) fn update_bytes(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u128).to_be_bytes());
    digest.update(value);
}

pub(crate) fn update_u64(digest: &mut Sha256, value: u64) {
    digest.update(value.to_be_bytes());
}

pub(crate) fn update_i64(digest: &mut Sha256, value: i64) {
    digest.update(value.to_be_bytes());
}

pub(crate) fn update_bool(digest: &mut Sha256, value: bool) {
    digest.update([u8::from(value)]);
}

pub(crate) fn finish(digest: Sha256) -> [u8; 32] {
    digest.finalize().into()
}

pub(crate) fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}
