//! Installed-service composition for guided research dataset preparation.

use std::{sync::Arc, time::Instant};

use market_squawk_services::RequestOrigin;
use tokio_util::sync::CancellationToken;

use crate::{
    ResearchService,
    application::{
        DatasetPreparationAuthority, DatasetPreparationError, DatasetPreparationOptions,
        DatasetPreparationPreview, DatasetPreparationPreviewRequest, DatasetPreparationReceipt,
        MacroContextReadCapability, PreparedFeatureDatasetBuild,
        lifecycle::WorkspaceRuntimeIdentity,
        market_calendar::CompletedMarketSessionReadCapability,
    },
};

/// Single process-owned guided preparation authority shared by installed transports and jobs.
#[derive(Debug)]
pub(super) struct InstalledResearchDatasetPreparation {
    authority: Arc<DatasetPreparationAuthority>,
}

impl InstalledResearchDatasetPreparation {
    pub(super) fn authority(&self) -> Arc<DatasetPreparationAuthority> {
        Arc::clone(&self.authority)
    }

    pub(super) fn new(
        research: Arc<ResearchService>,
        macro_context: MacroContextReadCapability,
        calendar: CompletedMarketSessionReadCapability,
        artifacts: Arc<dyn market_squawk_services::ArtifactRepository>,
    ) -> Self {
        Self {
            authority: Arc::new(DatasetPreparationAuthority::new(research, macro_context, calendar, artifacts)),
        }
    }

    pub(super) async fn options(
        &self,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<DatasetPreparationOptions, DatasetPreparationError> {
        self.authority.options(deadline, cancellation).await
    }

    pub(super) async fn preview(
        &self,
        request: DatasetPreparationPreviewRequest,
    ) -> Result<DatasetPreparationPreview, DatasetPreparationError> {
        self.authority.preview(request).await
    }

    pub(super) async fn prepare_investment_dataset(
        &self,
        instrument: market_squawk_domain::InstrumentId,
        source_cutoff: market_squawk_domain::Timestamp,
        intended_use: crate::application::DatasetPreparationUse,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        observed_at: market_squawk_domain::Timestamp,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<PreparedFeatureDatasetBuild, DatasetPreparationError> {
        self.authority
            .prepare_investment_dataset(
                instrument,
                source_cutoff,
                intended_use,
                origin,
                workspace,
                observed_at,
                deadline,
                cancellation,
            )
            .await
    }

    pub(super) async fn prepare_current_find_features(
        &self,
        population: &crate::application::PreparedFindPopulation,
        market_reader: &market_squawk_data::MarketDataInstrumentReadCapability,
        profile: &crate::application::analytical_profile::ValidatedAnalyticalProfile,
        source_cutoff: market_squawk_domain::Timestamp,
        source_actions: &crate::application::decision::current_find::RetainedCurrentFindSources,
        retained_partition_ends: Option<&[usize]>,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<crate::application::PreparedCurrentFindFeatures, DatasetPreparationError>
    {
        self.authority
            .prepare_current_find_features(
                population,
                market_reader,
                profile,
                source_cutoff,
                source_actions,
                retained_partition_ends,
                deadline,
                cancellation,
            )
            .await
    }

    pub(super) async fn prepare_current_find_feature_partition(
        &self,
        prepared: &crate::application::PreparedCurrentFindFeatures,
        ordinal: usize,
        calendar: &crate::application::market_calendar::CompletedMarketSessionRead,
        profile: &crate::application::analytical_profile::ValidatedAnalyticalProfile,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<
        crate::application::PreparedCurrentFindFeaturePartition,
        DatasetPreparationError,
    > {
        self.authority
            .prepare_current_find_feature_partition(
                prepared,
                ordinal,
                calendar,
                profile,
                deadline,
                cancellation,
            )
            .await
    }

    pub(super) fn rebind_current_find_partition(
        &self,
        prepared: &crate::application::PreparedCurrentFindFeatures,
        retained: &crate::application::decision::current_find::RetainedCurrentFindPartition,
    ) -> Result<
        crate::application::CurrentFindPartitionPreparationEvidence,
        DatasetPreparationError,
    > {
        self.authority
            .rebind_current_find_partition(prepared, retained)
    }

    pub(super) fn read_current_find_partition_dataset(
        &self, retained: &crate::application::decision::current_find::RetainedCurrentFindPartition,
        deadline: std::time::Instant, cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<Option<market_squawk_data::AnalyticalFeatureDataset>, crate::application::DatasetPreparationError> {
        self.authority.read_current_find_partition_dataset(retained, deadline, cancellation)
    }

    pub(super) fn read_current_find_partition(
        &self,
        evidence: crate::application::CurrentFindPartitionPreparationEvidence,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<crate::application::CurrentFindScreenPartition, DatasetPreparationError>
    {
        self.authority
            .read_current_find_partition(evidence, deadline, cancellation)
    }

    pub(super) fn consume(
        &self,
        receipt: DatasetPreparationReceipt,
        origin: RequestOrigin,
        workspace: WorkspaceRuntimeIdentity,
        now: Instant,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<PreparedFeatureDatasetBuild, DatasetPreparationError> {
        self.authority
            .consume(receipt, origin, workspace, now, deadline, cancellation)
    }
}
