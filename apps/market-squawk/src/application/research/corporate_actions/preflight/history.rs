//! Original-coordinate history requests through the existing registered source coordinator.

use super::*;
use crate::application::market_runtime::{
    AccountMarketSurface, PreparedMarketProviderConfigurationRequest,
};
use crate::application::{ResearchIngestCoordinator as _, ResearchSourceDiscoveryCoordinator as _};
use market_squawk_adapter_alpaca::{AlpacaAdjustment, AlpacaHistoricalEquityPreflightPlan};
use market_squawk_data::{
    CompleteMarketBarHistoryOutput, CompleteMarketBarHistoryRequest, DatasetSchemaRef, Sha256Digest,
};
use market_squawk_domain::{
    BarTimestampBasis, MarketBarAdjustment, MarketBarObservation, MarketBarSessionKind,
    ProviderInstrumentId, SchemaVersion, SourceIdentifier,
};
use serde::Deserialize;

pub(super) struct PublishedAnchorHistory {
    manifest: DatasetManifestRef,
    instrument: InstrumentId,
    range: (Timestamp, Timestamp),
    provider_instrument: ProviderInstrumentId,
    venue: VenueId,
    feed: SourceIdentifier,
    interval: SourceIdentifier,
    adjustment: MarketBarAdjustment,
    timestamp_basis: BarTimestampBasis,
    session_kind: MarketBarSessionKind,
    session_ruleset: SourceIdentifier,
}

impl SourceActionPreparationCapability {
    pub(super) async fn publish_history(
        &self,
        runtime: &AlpacaHistoricalRuntimeCapability,
        plan: AlpacaHistoricalEquityPreflightPlan,
        instrument: &MarketDataInstrumentRecord,
        original: &MarketBarObservation,
        context: &RequestContext,
    ) -> Result<PublishedAnchorHistory, ServiceError> {
        let exact = original.time_semantics().timestamped_period().ok_or(ServiceError::Unavailable)?;
        let published = self.publish_canonical_history(runtime, plan, instrument, context).await?;
        if published.provider_instrument != *original.provider_instrument_id()
            || original.context().provenance().venue_id() != Some(&published.venue)
            || published.feed != *original.feed() || published.interval != *original.interval()
            || published.timestamp_basis != exact.timestamp_basis()
            || published.session_kind != exact.session().kind()
            || published.session_ruleset != *exact.session().ruleset() {
            return Err(ServiceError::Unavailable);
        }
        Ok(published)
    }

    pub(super) async fn publish_canonical_history(
        &self, runtime: &AlpacaHistoricalRuntimeCapability,
        plan: AlpacaHistoricalEquityPreflightPlan, instrument: &MarketDataInstrumentRecord,
        context: &RequestContext,
    ) -> Result<PublishedAnchorHistory, ServiceError> {
        check(context)?;
        let provider_instrument = ProviderInstrumentId::try_from(plan.mapping().symbol()).map_err(|_| ServiceError::InvalidResult)?;
        let interval = plan.timeframe().provider_identifier().map_err(|_| ServiceError::InvalidResult)?;
        let range = (plan.start(), plan.end());
        let adjustment = match plan.adjustment() {
            AlpacaAdjustment::Raw => MarketBarAdjustment::Raw,
            AlpacaAdjustment::Split => MarketBarAdjustment::Split,
            _ => return Err(ServiceError::InvalidRequest),
        };
        let request = PreparedMarketProviderConfigurationRequest::try_new(
            AccountMarketSurface::AlpacaBasic,
            runtime.onboarding_session_id(),
            runtime.public_configuration_digest(),
            runtime.runtime_evidence_digest(),
            runtime.credential_generation(),
        )?;
        let receipt = self
            .runtime
            .admit_alpaca_historical_plan(
                request,
                plan,
                instrument.definition().clone(),
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        let authorized = self
            .runtime
            .authorize_alpaca_historical_plan_receipt(
                &receipt,
                context.deadline(),
                context.cancellation(),
            )
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        let provider_dataset = authorized.provider_dataset().clone();
        let analytical_dataset = authorized.analytical_dataset().clone();
        let semantics = authorized.series_semantics().clone();
        authorized
            .validate_current()
            .map_err(|_| ServiceError::Unavailable)?;
        // Discovery/extraction reacquire their own ordinary publication authority. Holding the
        // directory's exclusive barrier across those calls would deadlock its own source owner.
        drop(authorized);
        let profile = SourceIdentifier::try_from("alpaca.basic-market-data.historical-v1")
            .map_err(|_| ServiceError::Internal)?;
        let discovery = self
            .ingest
            .discover_registered_objects(
                &profile,
                &provider_dataset,
                None,
                NonZeroU16::MIN,
                context,
            )
            .await?;
        let rollback = DiscoveryRollback {
            owner: &self.ingest,
            discovery: &discovery,
        };
        let [object] = discovery.objects() else {
            return Err(ServiceError::InvalidResult);
        };
        if discovery.profile() != &profile || discovery.request().dataset() != &provider_dataset {
            return Err(ServiceError::InvalidResult);
        }
        let capabilities = crate::application::contracts::application_capabilities()
            .map_err(|_| ServiceError::Internal)?;
        let descriptor = capabilities
            .find("Research.IngestSource")
            .ok_or(ServiceError::Internal)?;
        let arguments = serde_json::json!({
            "provider":profile, "dataset":provider_dataset,
            "object":object.source_object().object_id(), "discoveryReceipt":object.discovery_receipt(),
            "confirm":true,
            "resultLimits":{"maximumItems":context.limits().maximum_result_items(),
                "maximumBytes":context.limits().maximum_result_bytes()},
        });
        let admitted = descriptor.admit(
            arguments
                .as_object()
                .cloned()
                .ok_or(ServiceError::Internal)?,
        )?;
        // Calls the sole coordinator directly under this operation's existing context. No child
        // job, second runtime, callback publisher, or native-authored capture is introduced.
        let publication = self
            .ingest
            .ingest(&admitted, context, context.limits())
            .await?;
        drop(rollback); // Receipt revocation is idempotent after the one-use ingest consumes it.
        let manifest: ManifestWire = serde_json::from_value(
            publication
                .structured_content()
                .get("manifest")
                .cloned()
                .ok_or(ServiceError::InvalidResult)?,
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let manifest = manifest.reconstruct()?;
        if manifest.dataset_id() != &analytical_dataset {
            return Err(ServiceError::InvalidResult);
        }
        runtime
            .require_current(context.deadline(), context.cancellation())
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?;
        Ok(PublishedAnchorHistory {
            manifest,
            instrument: instrument.definition().instrument_id(),
            range,
            provider_instrument,
            venue: VenueId::try_from("iex").map_err(|_| ServiceError::Internal)?,
            feed: SourceIdentifier::try_from("iex").map_err(|_| ServiceError::Internal)?,
            interval,
            adjustment,
            timestamp_basis: semantics.timestamp_basis(),
            session_kind: semantics.session().kind(),
            session_ruleset: semantics.session().ruleset().clone(),
        })
    }

    pub(super) async fn reopen_published_history(
        &self, original: PublishedAnchorHistory, cutoff: Timestamp, context: &RequestContext,
    ) -> Result<CompleteMarketBarHistoryOutput, ServiceError> {
        self.reopen_published_history_ref(&original, cutoff, context).await
    }

    pub(super) async fn reopen_published_history_ref(
        &self,
        original: &PublishedAnchorHistory,
        cutoff: Timestamp,
        context: &RequestContext,
    ) -> Result<CompleteMarketBarHistoryOutput, ServiceError> {
        let request = CompleteMarketBarHistoryRequest::try_exact(
            original.instrument,
            original.range.0,
            original.range.1,
            original.provider_instrument.clone(),
            original.venue.clone(),
            original.feed.clone(),
            original.interval.clone(),
            original.adjustment,
            original.timestamp_basis,
            original.session_kind,
            original.session_ruleset.clone(),
            cutoff,
            original.manifest.clone(),
        )
        .map_err(|_| ServiceError::InvalidResult)?;
        let read = self
            .research
            .analytical_reader()
            .read_complete_market_bar_history(
                request,
                context.deadline(),
                context.cancellation().clone(),
            )
            .await
            .map_err(|_| controlled(context, ServiceError::Unavailable))?
            .ok_or(ServiceError::Unavailable)?;
        self.research
            .rejoin_market_history_native_sessions(read, context.deadline(), context.cancellation())
            .await
            .map_err(|_| controlled(context, ServiceError::InvalidResult))
    }
}

struct DiscoveryRollback<'a> {
    owner: &'a Arc<ProductionResearchIngestCoordinator>,
    discovery: &'a crate::application::research::ingest::ResearchSourceDiscovery,
}
impl Drop for DiscoveryRollback<'_> {
    fn drop(&mut self) {
        let _ = self.owner.revoke_discovery_receipts(self.discovery);
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestWire {
    dataset_id: String,
    manifest_version: u64,
    schema: SchemaWire,
    content_hash: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SchemaWire {
    name: String,
    version: u16,
    fingerprint: String,
}
impl ManifestWire {
    fn reconstruct(self) -> Result<DatasetManifestRef, ServiceError> {
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset_id.as_str())
                .map_err(|_| ServiceError::InvalidResult)?,
            self.manifest_version,
            DatasetSchemaRef::try_new(
                self.schema.name,
                SchemaVersion::new(self.schema.version).map_err(|_| ServiceError::InvalidResult)?,
                decode_hex(&self.schema.fingerprint)?,
            )
            .map_err(|_| ServiceError::InvalidResult)?,
            Sha256Digest::new(decode_hex(&self.content_hash)?),
        )
        .map_err(|_| ServiceError::InvalidResult)
    }
}
fn decode_hex(value: &str) -> Result<[u8; 32], ServiceError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(ServiceError::InvalidResult);
    }
    let mut bytes = [0; 32];
    for (index, output) in bytes.iter_mut().enumerate() {
        *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| ServiceError::InvalidResult)?;
    }
    Ok(bytes)
}
