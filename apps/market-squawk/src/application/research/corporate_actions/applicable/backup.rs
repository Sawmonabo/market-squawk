//! Exact stopped-paper source dependencies; inert recipe parsing never grants source authority.
use super::*;
use market_squawk_services::ArtifactReference;

impl SourceAppliedCorporateActionPlanReference {
    /// Parses only the bounded original checkpoint field. Its recipe remains a value locator.
    pub(crate) fn from_paper_checkpoint(bytes: &[u8]) -> Result<Self, ApplicableActionPlanError> {
        if bytes.is_empty() || bytes.len() > 64 * 1024 {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        serde_json::from_slice(bytes).map_err(|_| ApplicableActionPlanError::InvalidEvidence)
    }
}
impl SourceAppliedCorporateActionReadCapability {
    /// Recovery-only reader of original immutable evidence. This cannot acquire or publish a
    /// calendar, and does not create a live currentness receipt or execution route.
    pub(crate) fn for_paper_backup(
        research: Arc<crate::ResearchService>,
        artifacts: Arc<dyn market_squawk_services::ArtifactRepository>,
    ) -> Self {
        let calendars = SourcePlanCalendarReader::Retained(Arc::new(
            crate::application::market_calendar::RetainedMarketSessionReadCapability::new(
                Arc::clone(&research),
            ),
        ));
        Self {
            research,
            calendars,
            artifacts: Some(artifacts),
        }
    }

    /// Reopens original captures, calendar, identities, recipe and financial content unchanged.
    /// This returns only the controlled artifact dependency after genuine covered-plan admission.
    /// Historical plans have no current recipe; their original raw dependencies are still included
    /// by the full catalog-owned immutable sealed closure in the existing SourceData component.
    pub(crate) async fn validate_paper_backup_source(
        &self,
        bytes: &[u8],
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Option<ArtifactReference>, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        let reference = SourceAppliedCorporateActionPlanReference::from_paper_checkpoint(bytes)?;
        let plan = self
            .read_reference(&reference, deadline, cancellation.clone())
            .await?
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        plan.covered_accounting_plan()?;
        if plan.source_reference()? != reference {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        check(deadline, cancellation)?;
        reference.current_recipe_artifact()
    }
}
