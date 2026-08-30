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
    },
};

/// Single process-owned guided preparation authority shared by installed transports and jobs.
#[derive(Debug)]
pub(super) struct InstalledResearchDatasetPreparation {
    authority: Arc<DatasetPreparationAuthority>,
}

impl InstalledResearchDatasetPreparation {
    pub(super) fn new(
        research: Arc<ResearchService>,
        macro_context: MacroContextReadCapability,
    ) -> Self {
        Self {
            authority: Arc::new(DatasetPreparationAuthority::new(research, macro_context)),
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
