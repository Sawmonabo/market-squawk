//! One original metadata/window capture, canonical raw/adjusted bars and native action columns.

mod staging;
pub use staging::*;

use super::*;
use crate::{
    TiingoEodActionError, TiingoEodCashUnitEvidence, TiingoEodExpectedSessionAuthority,
    TiingoEodInstrumentKind, TiingoSealedHistoryPage,
};
use market_squawk_domain::{CalendarDate, CorporateActionKind};
use market_squawk_sources::{
    CompleteMarketBarDateSessionV1, CompleteMarketBarDateWindowV1, ProviderCaptureSetReceipt,
    ProviderCaptureTerminalDisposition, RetainedMarketHistoryCalendarV1,
    RetainedMarketHistoryCashUnitV1, RetainedMarketHistoryNativeCoverageV1,
    RetainedMarketHistoryNormalizationV1,
};

const HISTORY_PURPOSE: &str = "tiingo-eod-complete-date-windows/v1";

/// Reconstructs the same native mapping from compact indexed history evidence.
pub fn reconstruct_eod_history_descriptor_mapping(
    graph: &TiingoEodHistoryDescriptor,
) -> Result<
    (TiingoEodInstrumentAuthority, TiingoEodContractEvidence),
    TiingoEodHistoryPublicationError,
> {
    let invalid = || TiingoEodHistoryPublicationError::CaptureMismatch;
    let native = graph.normalization();
    if graph.graph_purpose().as_str() != HISTORY_PURPOSE
        || graph.interval().as_str() != "tiingo-calendar-day"
    {
        return Err(invalid());
    }
    let instrument = TiingoEodInstrumentAuthority::try_new(
        graph.instrument_id(),
        graph.venue_id().clone(),
        graph.provider_instrument_id().clone(),
        crate::TiingoTicker::try_new(graph.provider_instrument_id().as_str())?,
        native.provider_exchange_code.clone(),
        if native.is_exchange_traded_fund {
            TiingoEodInstrumentKind::ExchangeTradedFund
        } else {
            TiingoEodInstrumentKind::Equity
        },
        native.instrument_definition.clone(),
        native.provider_mapping_evidence.clone(),
        native.resolved_at,
        native.currency,
    )?;
    let contract = TiingoEodContractEvidence::try_new(
        native.source_contract_revision.clone(),
        native.source_contract_evidence.clone(),
        native.native_schema_revision.clone(),
        native.native_schema_evidence.clone(),
        native.entitlement_generation_number,
        native.entitlement_generation.clone(),
        native.entitlement_evidence,
        native.adjusted_surface_evidence.clone(),
    )?;
    if contract.mapping_identity() != native.contract_identity
        || instrument
            .instrument_definition()
            .payload_evidence()
            .content_digest()
            != graph.instrument_revision_digest()
    {
        return Err(invalid());
    }
    Ok((instrument, contract))
}

/// Checks exact original native row values and their selected canonical surface/action column.
pub fn verify_eod_history_native_row(
    row: &crate::TiingoEodRow,
    selected: &str,
    retained: &[u8],
) -> Result<(), TiingoEodHistoryPublicationError> {
    let selected = match selected {
        "raw" => "raw",
        "adjusted" => "adjusted",
        "split" => "split",
        "dividend" => "dividend",
        _ => return Err(TiingoEodHistoryPublicationError::CaptureMismatch),
    };
    let expected = serde_json::to_vec(&TiingoNativeDailyRowV1::from_row(row, selected))
        .map_err(|_| TiingoEodHistoryPublicationError::CaptureMismatch)?;
    if expected != retained {
        return Err(TiingoEodHistoryPublicationError::CaptureMismatch);
    }
    Ok(())
}

/// Exact code-owned parser/model implementation identity used by source preparation.
pub fn tiingo_eod_native_schema_evidence() -> ExactPayloadEvidence {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/tiingo/native-decoder-schema/v1\0");
    for bytes in [
        include_bytes!("../decoder.rs").as_slice(),
        include_bytes!("../model.rs").as_slice(),
    ] {
        digest.update((bytes.len() as u64).to_be_bytes());
        digest.update(bytes);
    }
    ExactPayloadEvidence::from_content_digest(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        digest.finalize().into(),
    ))
}

#[derive(Debug, Error)]
pub enum TiingoEodHistoryPublicationError {
    #[error("Tiingo original native history and immutable capture do not match")]
    CaptureMismatch,
    #[error(transparent)]
    Latest(#[from] TiingoLatestPublicationError),
    #[error(transparent)]
    Eod(#[from] TiingoEodMapError),
    #[error(transparent)]
    Actions(#[from] TiingoEodActionError),
    #[error(transparent)]
    Capture(#[from] ProviderCaptureError),
    #[error(transparent)]
    Evidence(#[from] crate::TiingoHistoryEvidenceError),
    #[error(transparent)]
    Adapter(#[from] TiingoAdapterError),
    #[error(transparent)]
    Extraction(#[from] ExtractionError),
    #[error(transparent)]
    Native(#[from] ProviderNativeLineageError),
    #[error(transparent)]
    Revision(#[from] ObservedRevisionError),
}
