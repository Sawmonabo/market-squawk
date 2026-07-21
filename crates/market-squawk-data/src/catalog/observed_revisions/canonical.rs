//! Canonical assignment-batch and provider-order encodings.

use std::cmp::Ordering;

use market_squawk_domain::{DigestAlgorithm, EvidenceDigest, ResearchTemporalCoordinate};
use market_squawk_sources::{
    ObservedProviderOrder, ObservedRevisionBatch, ObservedRevisionError, ObservedRevisionRecord,
    ObservedVersionKind,
};
use sha2::{Digest as _, Sha256};

use super::{
    BATCH_CANONICAL_DOMAIN, BATCH_CANONICAL_VERSION, PROVIDER_ORDER_EVIDENCE_VERSION,
    StoredObservedRevision,
};

pub(super) struct EncodedProviderOrder {
    pub(super) version: Option<i64>,
    pub(super) coordinate_json: Option<String>,
    pub(super) tie_breaker: Option<Vec<u8>>,
}

pub(super) fn encoded_provider_order(
    order: Option<&ObservedProviderOrder>,
) -> Result<EncodedProviderOrder, ObservedRevisionError> {
    match order {
        None => Ok(EncodedProviderOrder {
            version: None,
            coordinate_json: None,
            tie_breaker: None,
        }),
        Some(order) => Ok(EncodedProviderOrder {
            version: Some(PROVIDER_ORDER_EVIDENCE_VERSION),
            coordinate_json: Some(
                serde_json::to_string(order.coordinate())
                    .map_err(|_| ObservedRevisionError::PersistenceUnavailable)?,
            ),
            tie_breaker: Some(order.exact_tie_breaker().to_vec()),
        }),
    }
}

pub(super) fn decode_provider_order(
    version: Option<i64>,
    coordinate_json: Option<String>,
    tie_breaker: Option<Vec<u8>>,
) -> Result<Option<ObservedProviderOrder>, ObservedRevisionError> {
    match (version, coordinate_json, tie_breaker) {
        (None, None, None) => Ok(None),
        (Some(PROVIDER_ORDER_EVIDENCE_VERSION), Some(json), Some(tie_breaker)) => {
            let coordinate: ResearchTemporalCoordinate = serde_json::from_str(&json)
                .map_err(|_| ObservedRevisionError::CorruptAuthorityState)?;
            if serde_json::to_string(&coordinate)
                .map_err(|_| ObservedRevisionError::CorruptAuthorityState)?
                != json
            {
                return Err(ObservedRevisionError::CorruptAuthorityState);
            }
            ObservedProviderOrder::try_new(coordinate, &tie_breaker)
                .map(Some)
                .map_err(|_| ObservedRevisionError::CorruptAuthorityState)
        }
        _ => Err(ObservedRevisionError::CorruptAuthorityState),
    }
}

pub(super) fn exact_record_match(
    stored: &StoredObservedRevision,
    requested: &ObservedRevisionRecord,
) -> bool {
    stored.version == *requested.version()
        && stored.semantic_payload == *requested.semantic_payload()
        && stored.provider_order.as_ref() == requested.provider_order()
}

pub(super) fn provider_order_cmp(
    left: &ObservedProviderOrder,
    right: &ObservedProviderOrder,
) -> Option<Ordering> {
    match left.coordinate().partial_cmp(right.coordinate())? {
        Ordering::Equal => Some(left.exact_tie_breaker().cmp(right.exact_tie_breaker())),
        ordering => Some(ordering),
    }
}

pub(super) fn require_digest(
    algorithm: i64,
    bytes: &[u8],
    expected: EvidenceDigest,
) -> Result<(), ObservedRevisionError> {
    if algorithm == 1
        && expected.algorithm() == DigestAlgorithm::Sha256
        && bytes == expected.bytes()
    {
        Ok(())
    } else {
        Err(ObservedRevisionError::CorruptAuthorityState)
    }
}

pub(super) fn version_kind_name(kind: ObservedVersionKind) -> &'static str {
    match kind {
        ObservedVersionKind::ProviderSupplied => "provider_supplied",
        ObservedVersionKind::LocallyObservedContent => "locally_observed_content",
    }
}

pub(super) fn canonical_batch_digest(
    batch: &ObservedRevisionBatch,
) -> Result<[u8; 32], ObservedRevisionError> {
    let mut digest = Sha256::new();
    push_framed_hash(&mut digest, BATCH_CANONICAL_DOMAIN)?;
    digest.update(BATCH_CANONICAL_VERSION.to_be_bytes());
    push_framed_hash(&mut digest, batch.source_id().as_str().as_bytes())?;
    digest.update(checked_u64(batch.input_len())?.to_be_bytes());
    digest.update(checked_u64(batch.unique_records().len())?.to_be_bytes());
    for record in batch.unique_records() {
        push_framed_hash(&mut digest, record.family().exact_bytes())?;
        digest.update([match record.version().kind() {
            ObservedVersionKind::ProviderSupplied => 1,
            ObservedVersionKind::LocallyObservedContent => 2,
        }]);
        push_framed_hash(&mut digest, record.version().exact_evidence())?;
        push_framed_hash(&mut digest, record.semantic_payload().exact_evidence())?;
        match record.provider_order() {
            None => digest.update([0]),
            Some(order) => {
                digest.update([1]);
                let coordinate = serde_json::to_vec(order.coordinate())
                    .map_err(|_| ObservedRevisionError::PersistenceUnavailable)?;
                push_framed_hash(&mut digest, &coordinate)?;
                push_framed_hash(&mut digest, order.exact_tie_breaker())?;
            }
        }
    }
    Ok(digest.finalize().into())
}

fn push_framed_hash(digest: &mut Sha256, value: &[u8]) -> Result<(), ObservedRevisionError> {
    digest.update(checked_u64(value.len())?.to_be_bytes());
    digest.update(value);
    Ok(())
}

fn checked_u64(value: usize) -> Result<u64, ObservedRevisionError> {
    u64::try_from(value).map_err(|_| ObservedRevisionError::ByteCountOverflow)
}
