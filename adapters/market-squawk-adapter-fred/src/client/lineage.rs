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
        | FredSourceError::InvalidConfiguration
        | FredSourceError::RevisionAuthority(_) => {
            ExtractionSourceError::Source(SourceError::InvalidProtocolState)
        }
        FredSourceError::BodyTooLarge { max } => {
            ExtractionSourceError::Source(SourceError::FrameTooLarge { max })
        }
        FredSourceError::Network => ExtractionSourceError::Source(SourceError::Network),
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
    returned: usize,
    total: usize,
    terminal: bool,
    page_digest: [u8; 32],
    metadata_digest: [u8; 32],
) -> Result<SourceIdentifier, FredSourceError> {
    SourceIdentifier::try_from(format!(
        "fred-page-v2:{offset}:{limit}:{returned}:{total}:{}:{}:{}",
        u8::from(terminal),
        lower_hex(page_digest),
        lower_hex(metadata_digest),
    ))
    .map_err(|_| FredSourceError::InvalidConfiguration)
}

/// Exact provider-page identity retained by one discovered FRED source object.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FredPageObjectIdentity {
    offset: usize,
    limit: usize,
    returned: usize,
    total: usize,
    terminal: bool,
    page_digest: [u8; 32],
    metadata_digest: [u8; 32],
}

impl FredPageObjectIdentity {
    /// Returns the exact zero-based provider offset.
    pub const fn offset(self) -> usize {
        self.offset
    }

    /// Returns the exact requested provider page limit.
    pub const fn limit(self) -> usize {
        self.limit
    }

    /// Returns the exact number of rows in this provider page.
    pub const fn returned(self) -> usize {
        self.returned
    }

    /// Returns the provider-declared total row count shared by the page chain.
    pub const fn total(self) -> usize {
        self.total
    }

    /// Returns whether this page exactly completes the provider-declared result.
    pub const fn terminal(self) -> bool {
        self.terminal
    }

    /// Returns the exact SHA-256 digest of the provider response body.
    pub const fn page_digest(self) -> [u8; 32] {
        self.page_digest
    }

    /// Returns the exact SHA-256 digest of the series-metadata response.
    pub const fn metadata_digest(self) -> [u8; 32] {
        self.metadata_digest
    }
}

pub(super) fn parse_object_id(
    value: &SourceIdentifier,
) -> Result<FredPageObjectIdentity, FredSourceError> {
    let mut fields = value.as_str().split(':');
    if fields.next() != Some("fred-page-v2") {
        return Err(FredSourceError::InvalidDataset);
    }
    let offset: usize = fields
        .next()
        .ok_or(FredSourceError::InvalidDataset)?
        .parse()
        .map_err(|_| FredSourceError::InvalidDataset)?;
    let limit: usize = fields
        .next()
        .ok_or(FredSourceError::InvalidDataset)?
        .parse()
        .map_err(|_| FredSourceError::InvalidDataset)?;
    let returned: usize = fields
        .next()
        .ok_or(FredSourceError::InvalidDataset)?
        .parse()
        .map_err(|_| FredSourceError::InvalidDataset)?;
    let total: usize = fields
        .next()
        .ok_or(FredSourceError::InvalidDataset)?
        .parse()
        .map_err(|_| FredSourceError::InvalidDataset)?;
    let terminal = match fields.next() {
        Some("0") => false,
        Some("1") => true,
        _ => return Err(FredSourceError::InvalidDataset),
    };
    let page_digest = fields.next().ok_or(FredSourceError::InvalidDataset)?;
    let metadata_digest = fields.next().ok_or(FredSourceError::InvalidDataset)?;
    let consumed = offset
        .checked_add(returned)
        .ok_or(FredSourceError::InvalidDataset)?;
    if fields.next().is_some()
        || limit == 0
        || returned == 0
        || returned > limit
        || consumed > total
        || terminal != (consumed == total)
        || page_digest.len() != 64
        || metadata_digest.len() != 64
    {
        return Err(FredSourceError::InvalidDataset);
    }
    Ok(FredPageObjectIdentity {
        offset,
        limit,
        returned,
        total,
        terminal,
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
