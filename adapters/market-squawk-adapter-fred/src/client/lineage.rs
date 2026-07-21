use market_squawk_domain::{
    DigestAlgorithm, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier,
    VersionPinnedSourceLocator,
};
use market_squawk_sources::{ExtractionSourceError, SourceError};
use sha2::{Digest, Sha256};

use super::FredSourceError;

pub(super) fn map_adapter_error(error: FredSourceError) -> ExtractionSourceError {
    match error {
        FredSourceError::DeadlineExceeded => ExtractionSourceError::DeadlineExceeded,
        FredSourceError::Cancelled => ExtractionSourceError::Cancelled,
        FredSourceError::InvalidApiKey
        | FredSourceError::InvalidDataset
        | FredSourceError::Protocol
        | FredSourceError::InvalidConfiguration => {
            ExtractionSourceError::Source(SourceError::InvalidProtocolState)
        }
        FredSourceError::BodyTooLarge | FredSourceError::Network => {
            ExtractionSourceError::Source(SourceError::Network)
        }
    }
}

pub(super) fn evidence_for_payload(
    payload: &[u8],
    public_url: &url::Url,
) -> Result<ExactPayloadEvidence, FredSourceError> {
    let digest: [u8; 32] = Sha256::digest(payload).into();
    let version = lower_hex(digest);
    Ok(ExactPayloadEvidence::with_version_pinned_locator(
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest),
        VersionPinnedSourceLocator::new(
            SourceIdentifier::try_from(public_url.as_str())
                .map_err(|_| FredSourceError::InvalidConfiguration)?,
            SourceIdentifier::try_from(version)
                .map_err(|_| FredSourceError::InvalidConfiguration)?,
        ),
    ))
}

pub(super) fn page_object_id(
    offset: usize,
    limit: usize,
    prior_revisions_for_first_observation: u32,
    page_digest: [u8; 32],
    metadata_digest: [u8; 32],
) -> Result<SourceIdentifier, FredSourceError> {
    SourceIdentifier::try_from(format!(
        "fred-page:{offset}:{limit}:{prior_revisions_for_first_observation}:{}:{}",
        lower_hex(page_digest),
        lower_hex(metadata_digest),
    ))
    .map_err(|_| FredSourceError::InvalidConfiguration)
}

pub(super) struct ParsedPageObjectId {
    pub(super) offset: usize,
    pub(super) limit: usize,
    pub(super) prior_revisions_for_first_observation: u32,
    pub(super) page_digest: [u8; 32],
    pub(super) metadata_digest: [u8; 32],
}

pub(super) fn parse_object_id(
    value: &SourceIdentifier,
) -> Result<ParsedPageObjectId, FredSourceError> {
    let mut fields = value.as_str().split(':');
    if fields.next() != Some("fred-page") {
        return Err(FredSourceError::InvalidDataset);
    }
    let offset = fields
        .next()
        .ok_or(FredSourceError::InvalidDataset)?
        .parse()
        .map_err(|_| FredSourceError::InvalidDataset)?;
    let limit = fields
        .next()
        .ok_or(FredSourceError::InvalidDataset)?
        .parse()
        .map_err(|_| FredSourceError::InvalidDataset)?;
    let prior_revisions_for_first_observation = fields
        .next()
        .ok_or(FredSourceError::InvalidDataset)?
        .parse::<u32>()
        .map_err(|_| FredSourceError::InvalidDataset)?;
    let page_digest = fields.next().ok_or(FredSourceError::InvalidDataset)?;
    let metadata_digest = fields.next().ok_or(FredSourceError::InvalidDataset)?;
    if fields.next().is_some()
        || limit == 0
        || usize::try_from(prior_revisions_for_first_observation)
            .map_or(true, |prior| prior > offset)
        || page_digest.len() != 64
        || metadata_digest.len() != 64
    {
        return Err(FredSourceError::InvalidDataset);
    }
    Ok(ParsedPageObjectId {
        offset,
        limit,
        prior_revisions_for_first_observation,
        page_digest: parse_lower_hex(page_digest)?,
        metadata_digest: parse_lower_hex(metadata_digest)?,
    })
}

fn parse_lower_hex(value: &str) -> Result<[u8; 32], FredSourceError> {
    let mut bytes = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        bytes[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(bytes)
}

fn hex_nibble(value: u8) -> Result<u8, FredSourceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(FredSourceError::InvalidDataset),
    }
}

fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}
