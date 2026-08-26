//! Exact Treasury page identity and refetch verification.

use market_squawk_domain::{
    DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, SourceIdentifier,
    Timestamp, VersionPinnedSourceLocator,
};
use market_squawk_sources::{
    DiscoveryRequest, ExtractionRequest, ExtractionSourceError, SourceError, SourceMetadata,
    SourceObject, payload_matches_exact_evidence,
};
use sha2::{Digest, Sha256};

use bytes::Bytes;

use crate::{TreasuryDailyRatePageRequest, TreasuryPageRequest};

const FISCAL_CHAIN_MAGIC: &[u8] = b"market-squawk/treasury-fiscal-page-chain\0\x01";
const FISCAL_PAGE_FRAME_OVERHEAD: usize = 2 + 8;
const FISCAL_CHAIN_TRAILER_BYTES: usize = 2 + 2;
pub(crate) const FISCAL_CHAIN_MEDIA_TYPE: &str =
    "application/vnd.market-squawk.treasury-fiscal-page-chain.v1";
pub(crate) const MAX_FISCAL_CHAIN_FRAMED_BYTES: usize =
    market_squawk_sources::MAX_PROVIDER_CAPTURE_BYTES as usize
        + FISCAL_CHAIN_MAGIC.len()
        + market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES * FISCAL_PAGE_FRAME_OVERHEAD
        + FISCAL_CHAIN_TRAILER_BYTES;

pub(super) struct FiscalChainFraming {
    bytes: Vec<u8>,
    page_count: u16,
}

impl FiscalChainFraming {
    pub(super) fn try_new() -> Result<Self, ExtractionSourceError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(FISCAL_CHAIN_MAGIC.len())
            .map_err(|_| invalid_protocol())?;
        bytes.extend_from_slice(FISCAL_CHAIN_MAGIC);
        Ok(Self {
            bytes,
            page_count: 0,
        })
    }

    pub(super) fn push(&mut self, body: &[u8]) -> Result<(), ExtractionSourceError> {
        let ordinal = self.page_count;
        if usize::from(ordinal) >= market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES {
            return Err(invalid_protocol());
        }
        let body_len = u64::try_from(body.len()).map_err(|_| invalid_protocol())?;
        let additional = FISCAL_PAGE_FRAME_OVERHEAD
            .checked_add(body.len())
            .ok_or_else(invalid_protocol)?;
        let framed_len = self
            .bytes
            .len()
            .checked_add(additional)
            .and_then(|value| value.checked_add(FISCAL_CHAIN_TRAILER_BYTES))
            .filter(|value| *value <= MAX_FISCAL_CHAIN_FRAMED_BYTES)
            .ok_or_else(invalid_protocol)?;
        self.bytes
            .try_reserve_exact(framed_len - self.bytes.len())
            .map_err(|_| invalid_protocol())?;
        self.bytes.extend_from_slice(&ordinal.to_be_bytes());
        self.bytes.extend_from_slice(&body_len.to_be_bytes());
        self.bytes.extend_from_slice(body);
        self.page_count = ordinal.checked_add(1).ok_or_else(invalid_protocol)?;
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<Bytes, ExtractionSourceError> {
        if self.page_count == 0 {
            return Err(invalid_protocol());
        }
        self.bytes.extend_from_slice(&u16::MAX.to_be_bytes());
        self.bytes.extend_from_slice(&self.page_count.to_be_bytes());
        Ok(Bytes::from(self.bytes))
    }
}

pub(crate) fn fiscal_chain_framed_evidence<'a>(
    bodies: impl IntoIterator<Item = &'a [u8]>,
) -> Result<(EvidenceDigest, u64), ExtractionSourceError> {
    let mut digest = Sha256::new();
    digest.update(FISCAL_CHAIN_MAGIC);
    let mut framed_bytes =
        u64::try_from(FISCAL_CHAIN_MAGIC.len()).map_err(|_| invalid_protocol())?;
    let mut raw_body_bytes = 0_u64;
    let mut count = 0_u16;
    for body in bodies {
        if usize::from(count) >= market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES {
            return Err(invalid_protocol());
        }
        let body_len = u64::try_from(body.len()).map_err(|_| invalid_protocol())?;
        raw_body_bytes = raw_body_bytes
            .checked_add(body_len)
            .filter(|value| *value <= market_squawk_sources::MAX_PROVIDER_CAPTURE_BYTES)
            .ok_or_else(invalid_protocol)?;
        digest.update(count.to_be_bytes());
        digest.update(body_len.to_be_bytes());
        digest.update(body);
        framed_bytes = framed_bytes
            .checked_add(u64::try_from(FISCAL_PAGE_FRAME_OVERHEAD).map_err(|_| invalid_protocol())?)
            .and_then(|value| value.checked_add(body_len))
            .ok_or_else(invalid_protocol)?;
        count = count.checked_add(1).ok_or_else(invalid_protocol)?;
    }
    if count == 0 {
        return Err(invalid_protocol());
    }
    digest.update(u16::MAX.to_be_bytes());
    digest.update(count.to_be_bytes());
    framed_bytes = framed_bytes
        .checked_add(u64::try_from(FISCAL_CHAIN_TRAILER_BYTES).map_err(|_| invalid_protocol())?)
        .filter(|value| {
            usize::try_from(*value).is_ok_and(|value| value <= MAX_FISCAL_CHAIN_FRAMED_BYTES)
        })
        .ok_or_else(invalid_protocol)?;
    Ok((
        EvidenceDigest::new(DigestAlgorithm::Sha256, digest.finalize().into()),
        framed_bytes,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ObjectKind {
    FiscalChain,
    DailyRate,
}

impl ObjectKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::FiscalChain => "fiscal-chain",
            Self::DailyRate => "daily-rate",
        }
    }

    fn parse(value: &str) -> Result<Self, ExtractionSourceError> {
        match value {
            "fiscal-chain" => Ok(Self::FiscalChain),
            "daily-rate" => Ok(Self::DailyRate),
            _ => Err(invalid_protocol()),
        }
    }
}

pub(super) trait PageIdentity {
    fn url(&self) -> &str;
    fn page_number(&self) -> usize;
    fn request_digest(&self) -> [u8; 32];
}

impl PageIdentity for TreasuryPageRequest {
    fn url(&self) -> &str {
        self.url()
    }

    fn page_number(&self) -> usize {
        self.page_number()
    }

    fn request_digest(&self) -> [u8; 32] {
        self.request_digest()
    }
}

pub(super) fn fiscal_chain_source_object(
    metadata: &SourceMetadata,
    request: &DiscoveryRequest,
    first_page: &TreasuryPageRequest,
    page_count: usize,
    framing: &Bytes,
    received_at: Timestamp,
) -> Result<SourceObject, ExtractionSourceError> {
    if page_count == 0
        || page_count > market_squawk_sources::MAX_PROVIDER_CAPTURE_PAGES
        || framing.len() > MAX_FISCAL_CHAIN_FRAMED_BYTES
    {
        return Err(invalid_protocol());
    }
    let payload_digest =
        EvidenceDigest::new(DigestAlgorithm::Sha256, Sha256::digest(framing).into());
    let expected_bytes = u64::try_from(framing.len()).map_err(|_| invalid_protocol())?;
    let object_id = SourceIdentifier::try_from(format!(
        "treasury-page:{}:{}:{}:{}",
        ObjectKind::FiscalChain.as_str(),
        page_count,
        lower_hex(first_page.request_digest()),
        lower_hex(payload_digest.bytes()),
    ))
    .map_err(|_| invalid_protocol())?;
    let locator = VersionPinnedSourceLocator::new(
        SourceIdentifier::try_from(first_page.url()).map_err(|_| invalid_protocol())?,
        SourceIdentifier::try_from(lower_hex(payload_digest.bytes()))
            .map_err(|_| invalid_protocol())?,
    );
    let evidence = ExactPayloadEvidence::with_version_pinned_locator(payload_digest, locator);
    SourceObject::try_new(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        request,
        object_id,
        SourceIdentifier::try_from(FISCAL_CHAIN_MEDIA_TYPE).map_err(|_| invalid_protocol())?,
        evidence,
        EffectiveInterval::new(received_at, None).map_err(|_| invalid_protocol())?,
        None,
        Some(expected_bytes),
    )
    .map_err(Into::into)
}

impl PageIdentity for TreasuryDailyRatePageRequest {
    fn url(&self) -> &str {
        self.url()
    }

    fn page_number(&self) -> usize {
        self.page_number()
    }

    fn request_digest(&self) -> [u8; 32] {
        self.request_digest()
    }
}

pub(super) fn source_object(
    metadata: &SourceMetadata,
    request: &DiscoveryRequest,
    page: &impl PageIdentity,
    payload: &[u8],
    received_at: Timestamp,
    media_type: &str,
    kind: ObjectKind,
) -> Result<SourceObject, ExtractionSourceError> {
    let payload_digest: [u8; 32] = Sha256::digest(payload).into();
    let object_id = SourceIdentifier::try_from(format!(
        "treasury-page:{}:{}:{}:{}",
        kind.as_str(),
        page.page_number(),
        lower_hex(page.request_digest()),
        lower_hex(payload_digest),
    ))
    .map_err(|_| invalid_protocol())?;
    let locator = VersionPinnedSourceLocator::new(
        SourceIdentifier::try_from(page.url()).map_err(|_| invalid_protocol())?,
        SourceIdentifier::try_from(lower_hex(payload_digest)).map_err(|_| invalid_protocol())?,
    );
    let evidence = ExactPayloadEvidence::with_version_pinned_locator(
        EvidenceDigest::new(DigestAlgorithm::Sha256, payload_digest),
        locator,
    );
    let effective = EffectiveInterval::new(received_at, None).map_err(|_| invalid_protocol())?;
    SourceObject::try_new(
        metadata.source_id().clone(),
        metadata.revision().clone(),
        request,
        object_id,
        SourceIdentifier::try_from(media_type).map_err(|_| invalid_protocol())?,
        evidence,
        effective,
        None,
        Some(u64::try_from(payload.len()).map_err(|_| invalid_protocol())?),
    )
    .map_err(ExtractionSourceError::from)
}

pub(super) struct ParsedObjectId {
    pub(super) kind: ObjectKind,
    pub(super) page_number: usize,
    request_digest: [u8; 32],
    pub(super) payload_digest: [u8; 32],
}

impl ParsedObjectId {
    pub(super) fn parse(value: &SourceIdentifier) -> Result<Self, ExtractionSourceError> {
        let mut fields = value.as_str().split(':');
        if fields.next() != Some("treasury-page") {
            return Err(invalid_protocol());
        }
        let kind = ObjectKind::parse(fields.next().ok_or_else(invalid_protocol)?)?;
        let page_number = fields
            .next()
            .ok_or_else(invalid_protocol)?
            .parse()
            .map_err(|_| invalid_protocol())?;
        let request_digest = parse_lower_hex(fields.next().ok_or_else(invalid_protocol)?)?;
        let payload_digest = parse_lower_hex(fields.next().ok_or_else(invalid_protocol)?)?;
        if fields.next().is_some() || (kind == ObjectKind::FiscalChain && page_number == 0) {
            return Err(invalid_protocol());
        }
        Ok(Self {
            kind,
            page_number,
            request_digest,
            payload_digest,
        })
    }

    pub(super) fn verify_request(&self, actual: [u8; 32]) -> Result<(), ExtractionSourceError> {
        if self.request_digest != actual {
            return Err(invalid_protocol());
        }
        Ok(())
    }
}

pub(super) fn verify_refetched_fiscal_chain(
    request: &ExtractionRequest,
    expected_digest: [u8; 32],
    expected_pages: usize,
    framing: &Bytes,
) -> Result<(), ExtractionSourceError> {
    let actual: [u8; 32] = Sha256::digest(framing).into();
    let actual_bytes = u64::try_from(framing.len()).map_err(|_| invalid_protocol())?;
    if expected_pages == 0
        || actual != expected_digest
        || !payload_matches_exact_evidence(framing, request.object().evidence())
        || request.object().expected_bytes() != Some(actual_bytes)
    {
        return Err(ExtractionSourceError::Source(
            SourceError::GenerationResynchronizationRequired,
        ));
    }
    Ok(())
}

pub(super) fn verify_refetched_object(
    request: &ExtractionRequest,
    expected_digest: [u8; 32],
    payload: &[u8],
) -> Result<(), ExtractionSourceError> {
    let actual: [u8; 32] = Sha256::digest(payload).into();
    let actual_bytes = u64::try_from(payload.len()).map_err(|_| invalid_protocol())?;
    if actual != expected_digest
        || !payload_matches_exact_evidence(payload, request.object().evidence())
        || request
            .object()
            .expected_bytes()
            .is_some_and(|expected| expected != actual_bytes)
    {
        return Err(ExtractionSourceError::Source(
            SourceError::GenerationResynchronizationRequired,
        ));
    }
    Ok(())
}

fn parse_lower_hex(value: &str) -> Result<[u8; 32], ExtractionSourceError> {
    if value.len() != 64 {
        return Err(invalid_protocol());
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        output[index] = (hex_nibble(pair[0])? << 4) | hex_nibble(pair[1])?;
    }
    Ok(output)
}

fn hex_nibble(value: u8) -> Result<u8, ExtractionSourceError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(invalid_protocol()),
    }
}

pub(super) fn lower_hex(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

pub(super) fn invalid_protocol() -> ExtractionSourceError {
    ExtractionSourceError::Source(SourceError::InvalidProtocolState)
}
