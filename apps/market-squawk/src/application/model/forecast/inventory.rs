//! Demand reads and bounded scans of immutable forecast descriptors.
use super::*;
use market_squawk_data::{
    ForecastInventoryCatalogCapability, ForecastInventoryHead, ForecastInventoryLookup,
};
use persistence::StoredVintageRecord;

impl ForecastApplicationService {
    pub(super) async fn read_stored(
        &self,
        stored: &StoredVintageRecord,
        context: &ArtifactReadContext,
    ) -> Result<VintageRecord, ForecastApplicationError> {
        let artifact = self
            .artifacts
            .read(
                ArtifactReadRequest::try_new(
                    stored.artifact_reference()?,
                    NonZeroUsize::new(MAXIMUM_FORECAST_ARTIFACT_BYTES)
                        .ok_or(ForecastApplicationError::InvalidLimits)?,
                )?,
                context.clone(),
            )
            .await?;
        stored.reopen(&artifact)
    }
    pub(super) async fn lookup_vintage(
        &self,
        key: ForecastInventoryLookup<'_>,
        context: &ArtifactReadContext,
    ) -> Result<Option<VintageRecord>, ForecastApplicationError> {
        let fence = self.catalog.head()?;
        let Some(bytes) = self.catalog.get(fence, key)? else {
            return Ok(None);
        };
        self.read_stored(&StoredVintageRecord::decode(&bytes)?, context)
            .await
            .map(Some)
    }
    pub(super) fn stored_detail(
        &self,
        key: ForecastInventoryLookup<'_>,
        context: &ArtifactReadContext,
    ) -> Result<Value, ForecastApplicationError> {
        context.ensure_live()?;
        let fence = self.catalog.head()?;
        let bytes = self
            .catalog
            .get(fence, key)?
            .ok_or(ForecastApplicationError::NotFound)?;
        let stored = StoredVintageRecord::decode(&bytes)?;
        let mut outcomes = Vec::new();
        let mut after = 0;
        loop {
            context.ensure_live()?;
            let page = self
                .catalog
                .outcomes(fence, after, 32, Some(&stored.vintage_id))?;
            if page.is_empty() {
                break;
            }
            for (sequence, bytes) in page {
                if outcomes.len() >= market_squawk_modeling::MAX_FORECAST_POINTS {
                    return Err(ForecastApplicationError::CorruptIndex);
                }
                let outcome = OutcomeRecord::decode_stored(&bytes)?;
                if outcome.vintage_id != stored.vintage_id {
                    return Err(ForecastApplicationError::CorruptIndex);
                }
                outcomes.push(outcome);
                after = sequence;
            }
        }
        stored.product_detail(drift_monitoring_parts(
            &outcomes,
            &stored.vintage_id,
            stored.decimal_scale(),
            |value, scale| stored.product_amount(value, scale),
        )?)
    }
    pub(super) async fn selected_index(
        &self,
        token: Uuid,
        context: &ArtifactReadContext,
    ) -> Result<ForecastIndex, ForecastApplicationError> {
        let vintage = self
            .lookup_vintage(ForecastInventoryLookup::Token(&token.to_string()), context)
            .await?
            .ok_or(ForecastApplicationError::NotFound)?;
        self.index_for_vintage(vintage)
    }
    pub(super) fn index_for_vintage(
        &self,
        vintage: VintageRecord,
    ) -> Result<ForecastIndex, ForecastApplicationError> {
        let mut index = ForecastIndex::default();
        let fence = self.catalog.head()?;
        let mut after = 0;
        loop {
            let page = self
                .catalog
                .outcomes(fence, after, 32, Some(&vintage.vintage_id))?;
            if page.is_empty() {
                break;
            }
            for (sequence, bytes) in page {
                if index.outcomes.len() >= market_squawk_modeling::MAX_FORECAST_POINTS {
                    return Err(ForecastApplicationError::CorruptIndex);
                }
                index
                    .outcomes
                    .push(OutcomeRecord::decode(&bytes, &vintage)?);
                after = sequence;
            }
        }
        // Each validated outcome is unique by one of this exact path's bounded target points.
        index.vintages.push(vintage);
        Ok(index)
    }
    pub(super) async fn latest_selection(
        &self,
        instrument: InstrumentId,
        as_of: Timestamp,
        horizon: Option<NonZeroU64>,
        context: &ForecastEvidenceReadContext,
    ) -> Result<ForecastIndexSelection, ForecastApplicationError> {
        let fence = self.catalog.head()?;
        let population =
            usize::try_from(fence.vintages).map_err(|_| ForecastApplicationError::Capacity)?;
        let mut after = 0;
        let mut eligible = 0_usize;
        let mut selected: Option<ForecastIndexSelection> = None;
        loop {
            context.ensure_live()?;
            let page =
                self.catalog
                    .vintages(fence, after, 1, false, Some(&instrument.to_string()))?;
            let Some((sequence, bytes)) = page.into_iter().next() else {
                break;
            };
            after = sequence;
            let stored = StoredVintageRecord::decode(&bytes)?;
            let vintage = self.read_stored(&stored, &context.artifact).await?;
            let mut singleton = ForecastIndex::default();
            singleton.vintages.push(vintage);
            let candidate = match horizon {
                Some(horizon) => singleton.latest_valid_exact_horizon_price_for_instrument(
                    instrument,
                    horizon,
                    as_of,
                    NonZeroUsize::MIN,
                ),
                None => singleton.latest_valid_for_instrument(instrument, as_of, NonZeroUsize::MIN),
            };
            let candidate = match candidate {
                Ok(value) => value,
                Err(ForecastApplicationError::NotFound) => continue,
                Err(error) => return Err(error),
            };
            eligible = eligible
                .checked_add(1)
                .ok_or(ForecastApplicationError::Capacity)?;
            if selected.as_ref().is_none_or(|current| {
                persistence::compare_selection_priority(&candidate.vintage, &current.vintage)
                    .is_gt()
            }) {
                selected = Some(candidate);
            }
        }
        let mut selected = selected.ok_or(ForecastApplicationError::NotFound)?;
        let mut body = selected.receipt.body;
        body.considered_vintage_count = population;
        body.inventory_vintage_count = population;
        body.eligible_vintage_count = eligible;
        body.competing_eligible_vintage_count = eligible - 1;
        selected.receipt = ForecastSelectionReceipt::try_new(body)?;
        Ok(selected)
    }
}

/// Exact serialized immutable prefixes for backup; no forecast history is retained in memory.
pub(super) struct ForecastBackupInventory {
    pub(super) catalog: ForecastInventoryCatalogCapability,
    pub(super) head: ForecastInventoryHead,
}
