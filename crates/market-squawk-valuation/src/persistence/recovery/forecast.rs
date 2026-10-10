//! Artifact-authenticated forecast reconstruction without a persistent secondary registry.

use super::*;
use crate::checked_add;
use crate::evidence::{ForecastValuationReference, ForecastValuationResolver};
use market_squawk_data::ResearchUse;

const MAXIMUM_RECOVERY_SOURCES: usize = 512;

pub(crate) async fn recover_with_forecasts(
    snapshot: &FairValueCatalogSnapshot,
    resolver: &dyn ForecastValuationResolver,
    recovery_at: Timestamp,
    maximum_bytes: usize,
) -> Result<RecoveredState, FairValueError> {
    validate_operation_coverage(snapshot)?;
    let mut requests = BTreeMap::new();
    for record in snapshot
        .records()
        .iter()
        .filter(|record| record.kind() == FairValueRecordKind::Evidence)
    {
        let payload: EvidencePayload = canonical(record.payload())?;
        collect_requests(payload.origin, &mut requests, false)?;
    }
    let mut sources = ForecastSources::new();
    let mut retained_bytes = 0_usize;
    for (id, reference) in requests {
        let (source, authorization) = resolver.resolve(&reference).await?;
        if source.reference() != &reference {
            return Err(FairValueError::CorruptPersistence);
        }
        if authorization.research_use() != ResearchUse::LocalAnalysis
            || authorization.expires_at() <= recovery_at
            || reference.parent_manifests().iter().any(|manifest| {
                !authorization
                    .graph()
                    .nodes()
                    .iter()
                    .any(|node| node.manifest() == manifest)
            })
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        retained_bytes = checked_add(retained_bytes, source.retained_bytes())?;
        if retained_bytes > maximum_bytes {
            return Err(FairValueError::LimitExceeded {
                resource: "forecast recovery evidence bytes",
                observed: retained_bytes,
                limit: maximum_bytes,
            });
        }
        let _consumed_recovery_permit = authorization.into_permit();
        sources.insert(id, std::sync::Arc::new(source));
    }
    recover_resolved(snapshot, &sources)
}

fn collect_requests(
    origin: OriginPayload,
    requests: &mut BTreeMap<[u8; 32], ForecastValuationReference>,
    nested: bool,
) -> Result<(), FairValueError> {
    match origin {
        OriginPayload::ForecastDistribution {
            source,
            ordinal,
            financial_origin,
        } => {
            if ordinal.is_some_and(|index| index >= 512 || financial_origin) {
                return Err(FairValueError::CorruptPersistence);
            }
            let reference = reference_from_payload(*source)?;
            let id = reference.identity().bytes();
            if let Some(previous) = requests.get(&id) {
                if previous != &reference {
                    return Err(FairValueError::CorruptPersistence);
                }
            } else {
                if requests.len() >= MAXIMUM_RECOVERY_SOURCES {
                    return Err(FairValueError::LimitExceeded {
                        resource: "forecast recovery sources",
                        observed: requests.len() + 1,
                        limit: MAXIMUM_RECOVERY_SOURCES,
                    });
                }
                requests.insert(id, reference);
            }
        }
        OriginPayload::AutomaticValuation { receipt } => {
            if nested || receipt.inputs.len() > 512 {
                return Err(FairValueError::CorruptPersistence);
            }
            for input in receipt.inputs {
                collect_requests(input.evidence.origin, requests, true)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn reference_from_payload(
    value: ForecastReferencePayload,
) -> Result<ForecastValuationReference, FairValueError> {
    for bytes in [
        value.identity,
        value.distribution_identity,
        value.vintage_id,
        value.forecast_artifact_hash,
        value.metadata_hash,
        value.serving_graph,
        value.serving_query,
        value.serving_result,
        value.serving_feature,
    ] {
        if bytes == [0; 32] {
            return Err(FairValueError::CorruptPersistence);
        }
    }
    let source_origin = match (
        value.origin_bar_digest,
        value.financial_epoch_digest,
        value.current_price_epoch_digest,
    ) {
        (Some(bytes), None, None) if bytes != [0; 32] => {
            crate::ForecastValuationOriginIdentity::CompletedBar(digest(1, bytes)?)
        }
        (None, Some(bytes), None) if bytes != [0; 32] => {
            crate::ForecastValuationOriginIdentity::FinancialEpoch(digest(1, bytes)?)
        }
        (None, None, Some(bytes)) if bytes != [0; 32] => {
            crate::ForecastValuationOriginIdentity::CurrentPriceEpoch(digest(1, bytes)?)
        }
        _ => return Err(FairValueError::CorruptPersistence),
    };
    if value.selected_at_ns < value.knowledge_at_ns {
        return Err(FairValueError::CorruptPersistence);
    }
    if value.parent_manifests.is_empty()
        || value.parent_manifests.len() > market_squawk_modeling::MAX_FORECAST_SERVING_PARENTS + 1
    {
        return Err(FairValueError::CorruptPersistence);
    }
    Ok(ForecastValuationReference {
        identity: digest(1, value.identity)?,
        distribution_identity: Sha256Digest::new(value.distribution_identity),
        vintage_id: Sha256Digest::new(value.vintage_id),
        forecast_artifact_hash: Sha256Digest::new(value.forecast_artifact_hash),
        metadata_hash: Sha256Digest::new(value.metadata_hash),
        instrument_id: instrument(&value.instrument_id)?,
        training_manifest: manifest_from_payload(value.training_manifest)?,
        serving_manifest: manifest_from_payload(value.serving_manifest)?,
        parent_manifests: value
            .parent_manifests
            .into_iter()
            .map(manifest_from_payload)
            .collect::<Result<Vec<_>, _>>()?
            .into_boxed_slice(),
        serving_source: SourceId::try_from(value.serving_source)
            .map_err(|_| FairValueError::CorruptPersistence)?,
        serving_graph: digest(1, value.serving_graph)?,
        serving_query: digest(1, value.serving_query)?,
        serving_result: digest(1, value.serving_result)?,
        serving_feature: Sha256Digest::new(value.serving_feature),
        source_origin,
        knowledge_at: Timestamp::from_unix_nanos(value.knowledge_at_ns),
        selected_at: Timestamp::from_unix_nanos(value.selected_at_ns),
    })
}
