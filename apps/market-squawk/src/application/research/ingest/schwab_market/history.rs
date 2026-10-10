//! Exact daily history publication through the existing research authority.
use super::*;
impl SchwabMarketPublicationClosure {
    /// Publishes an originally sealed response while retaining account, instrument, and calendar
    /// currentness through the common catalog commit.
    #[allow(
        clippy::too_many_arguments,
        reason = "transport, capture, mapping, publication, and authority coordinates remain exact"
    )]
    pub(crate) async fn publish_already_sealed_daily_price_history(
        &self,
        sealed: SchwabSealedRestResponse,
        request: SchwabDailyPriceHistoryPublicationRequest,
        oauth_epoch: SchwabOAuthPublicationEpoch,
        account: crate::provider_activation::ProviderAccountPublicationAuthority,
        record: market_squawk_data::MarketDataInstrumentRecord,
        observed_at: Timestamp,
        analytical_dataset: DatasetId,
        cancellation: CancellationToken,
        deadline: Instant,
    ) -> Result<SchwabPriceHistoryPublicationReceipt, SchwabMarketPublicationError> {
        let oauth = oauth_epoch.receipt();
        self.validate_capture_binding(
            sealed.persisted_receipt().capture().source_id(),
            sealed.persisted_receipt().capture().metadata_revision(),
        )?;
        self.rights
            .validate_subject(Some(sealed.persisted_receipt().capture().dataset()))
            .map_err(|_| SchwabMarketPublicationError::AuthorityInvalid)?;
        self.validate_oauth_authority(oauth, observed_at)?;
        if sealed.route() != ReadOnlyRoute::PriceHistory
            || sealed.receipt().credential_authority() != oauth.credential_authority()
            || sealed.receipt().token_generation() != oauth.generation()
            || timestamp_from_unix_millis(sealed.receipt().received_at_unix_millis())? > observed_at
        {
            return Err(SchwabMarketPublicationError::FamilyMismatch);
        }
        if cancellation.is_cancelled() || Instant::now() >= deadline {
            return Err(SchwabMarketPublicationError::Cancelled);
        }
        let publication = sealed.into_daily_price_history_publication(request)?;
        let calendar = publication.calendar_range();
        if calendar.instrument_id() != record.definition().instrument_id()
            || calendar.instrument_revision_digest() != record.revision_digest()
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let lease = self
            .acquire_attempt_publication_lease(oauth_epoch, observed_at, &cancellation)
            .await?;
        let market_data = publication.market_data().clone();
        let publication_checked_at = trusted_now()?;
        let (revisions, binding) = publication.into_parts(publication_checked_at)?;
        binding.validate()?;
        self.validate_capture_binding(
            binding.capture_evidence().source_id(),
            binding.capture_evidence().metadata_revision(),
        )?;
        if binding.native_lineage().schema().implementation()
            != ProviderNativeLineageImplementation::SchwabRestMarketDataV1
        {
            return Err(SchwabMarketPublicationError::AuthorityInvalid);
        }
        let binding_digest = binding.evidence_digest().evidence();
        let payload_digest = extraction_provider_payload_digest(binding.batch());
        let rights = self
            .rights
            .decision(payload_digest, observed_at)
            .map_err(|_error| SchwabMarketPublicationError::AuthorityInvalid)?;
        let precommit: Arc<dyn IngestPrecommitAuthority> = Arc::new(HistoryPrecommit {
            lease,
            account,
            record,
            calendar,
            catalog: self.research.market_data_instruments(),
            deadline,
            cancellation: cancellation.clone(),
        });
        let ingest = ResearchIngestRequest::with_provider_publication(
            self.generation.metadata().clone(),
            rights,
            analytical_dataset,
            binding,
            revisions,
        )?
        .with_precommit_authority(precommit);
        let committed = self.research.ingest(ingest, cancellation).await?;
        Ok(SchwabPriceHistoryPublicationReceipt {
            committed,
            binding_digest,
            market_data,
        })
    }
}

#[derive(Debug)]
struct HistoryPrecommit {
    lease: Arc<SchwabMarketPublicationLease>,
    account: crate::provider_activation::ProviderAccountPublicationAuthority,
    record: market_squawk_data::MarketDataInstrumentRecord,
    calendar: Arc<dyn market_squawk_adapter_schwab::SchwabDailyPriceHistoryCalendarRangeReceipt>,
    catalog: market_squawk_data::MarketDataInstrumentReadCapability,
    deadline: Instant,
    cancellation: CancellationToken,
}
impl IngestPrecommitAuthority for HistoryPrecommit {
    fn validate_precommit(&self) -> Result<(), IngestError> {
        if self.cancellation.is_cancelled() || Instant::now() >= self.deadline {
            return Err(IngestError::PublicationAuthorityRevoked);
        }
        let at = trusted_now().map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        let interval = self.record.definition().effective_interval();
        if self.record.published_at() > at
            || interval.starts_at() > at
            || interval.ends_at().is_some_and(|end| at >= end)
            || !self.calendar.validate_current_at(at)
        {
            return Err(IngestError::PublicationAuthorityRevoked);
        }
        self.lease.validate_precommit()?;
        self.account
            .require_current()
            .map_err(|_| IngestError::PublicationAuthorityRevoked)
    }
    fn validate_catalog_precommit(
        &self,
        catalog: &market_squawk_data::CatalogAuthority,
    ) -> Result<(), IngestError> {
        self.validate_precommit()?;
        self.lease.validate_catalog_precommit(catalog)?;
        self.account
            .require_catalog_current(catalog)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)?;
        self.catalog
            .require_current_in_catalog(catalog, &self.record, self.deadline, &self.cancellation)
            .map_err(|_| IngestError::PublicationAuthorityRevoked)
    }
}
