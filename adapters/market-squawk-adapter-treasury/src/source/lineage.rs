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

use crate::{TreasuryPageRequest, TreasuryYieldCurvePageRequest};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ObjectKind {
    Fiscal,
    Yield,
}

impl ObjectKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Fiscal => "fiscal",
            Self::Yield => "yield",
        }
    }

    fn parse(value: &str) -> Result<Self, ExtractionSourceError> {
        match value {
            "fiscal" => Ok(Self::Fiscal),
            "yield" => Ok(Self::Yield),
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

impl PageIdentity for TreasuryYieldCurvePageRequest {
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
        if fields.next().is_some()
            || (kind == ObjectKind::Fiscal && page_number == 0)
            || (kind == ObjectKind::Yield && page_number != 0)
        {
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
