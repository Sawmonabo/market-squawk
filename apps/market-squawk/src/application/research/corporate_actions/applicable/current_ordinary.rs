//! Compact original current-source recipe and physical reconstruction through existing owners.

use super::*;
use market_squawk_data::CurrentOrdinaryActionSourceRead;
use market_squawk_domain::DigestAlgorithm;
use market_squawk_services::{
    ArtifactPublication, ArtifactPublicationContext, ArtifactReadContext, ArtifactReadRequest,
    ArtifactReference, ArtifactRepository,
};

// Exact worst-case canonical JSON: envelope461 bytes +16000 entries of399 bytes +15999 commas.
// Entries retain two32-byte SHA256 values and one UUID; original rows remain in source artifacts.
pub(super) const MAX_CURRENT_RECIPE_BYTES: usize = 6_400_460;

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CurrentOrdinaryRecipeReference {
    id: String,
    sha256: String,
    bytes: usize,
}
impl CurrentOrdinaryRecipeReference {
    pub(super) fn valid(&self) -> bool {
        self.bytes <= MAX_CURRENT_RECIPE_BYTES && self.artifact().is_ok()
    }
    pub(super) fn artifact(&self) -> Result<ArtifactReference, ApplicableActionPlanError> {
        ArtifactReference::try_new(
            self.id.as_str(),
            self.sha256.as_str(),
            self.bytes,
            "application/json",
        )
        .map_err(|_| ApplicableActionPlanError::InvalidEvidence)
    }
    fn from_artifact(reference: &ArtifactReference) -> Self {
        Self {
            id: reference.id().to_owned(),
            sha256: reference.sha256().to_owned(),
            bytes: reference.byte_count(),
        }
    }
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct CurrentRecipe {
    version: u16,
    source_binding: EvidenceDigest,
    knowledge_cutoff: Timestamp,
    valuation_cutoff: Timestamp,
    coverage_digest: EvidenceDigest,
    reads: Vec<CurrentOrdinaryReference>,
}

pub(super) struct CurrentOrdinaryCoverage {
    pub(super) references: Vec<CurrentOrdinaryReference>,
    pub(super) digest: EvidenceDigest,
    pub(super) recipe: Option<CurrentOrdinaryRecipeReference>,
}
/// Only original coordinates are persisted; native bodies remain in controlled source artifacts.
#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CurrentOrdinaryReference {
    origin_content: EvidenceDigest,
    binding: EvidenceDigest,
    subject: InstrumentId,
}
impl CurrentOrdinaryReference {
    pub(super) fn valid(&self) -> bool {
        [self.origin_content, self.binding]
            .into_iter()
            .all(|digest| {
                digest.algorithm() == DigestAlgorithm::Sha256 && digest.bytes() != [0; 32]
            })
    }
    fn from_read(read: &CurrentOrdinaryActionSourceRead) -> Self {
        Self {
            origin_content: EvidenceDigest::new(
                DigestAlgorithm::Sha256,
                read.manifest().content_hash().bytes(),
            ),
            binding: read.binding_digest(),
            subject: read.instrument().definition().instrument_id(),
        }
    }
}

impl SourceAppliedCorporateActionPlan {
    /// Uses original current economic-date reads. It cannot borrow a historical history proof,
    /// replace source knowledge, or turn an unavailable monetary unit into an empty action list.
    pub(crate) fn with_current_ordinary_reads(
        mut self,
        mut reads: Vec<CurrentOrdinaryActionSourceRead>,
        limits: CorporateActionLimits,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<Self, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        if self.ordinary.is_some() || self.current_ordinary.is_some() || self.anchor.is_some() {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        reads.sort_by_key(|read| {
            (
                read.instrument().definition().instrument_id(),
                read.interval(),
                read.family(),
            )
        });
        self.plan = CorporateActionPlan::try_from_current_ordinary_source_reads(
            &self.source,
            &self.query_identity,
            self.calendar.source_action_calendar(),
            &reads,
            &self.requested_instruments.iter().copied().collect(),
            self.interval,
            self.plan.policy(),
            self.payment_policy,
            self.plan.valuation_cutoff(),
            self.evaluated_at,
            limits,
            deadline,
            cancellation,
        )
        .map_err(|error| map_source_plan_error(error, deadline, cancellation))?;
        let coverage = self
            .plan
            .source_coverage()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        self.outside_window = coverage.outside_window().to_vec().into_boxed_slice();
        self.unresolved = coverage.unresolved().to_vec().into_boxed_slice();
        self.current_ordinary = Some(CurrentOrdinaryCoverage {
            references: reads
                .iter()
                .map(CurrentOrdinaryReference::from_read)
                .collect(),
            recipe: None,
            digest: coverage.evidence_digest(),
        });
        // Source bytes are already charged once in the existing plan; drop read owners here.
        check(deadline, cancellation)?;
        Ok(self)
    }
}

impl SourceAppliedCorporateActionReadCapability {
    /// Reuses the installed content-addressed owner; this creates no filesystem or worker authority.
    pub(crate) fn with_artifact_repository(
        mut self,
        artifacts: Arc<dyn ArtifactRepository>,
    ) -> Self {
        self.artifacts = Some(artifacts);
        self
    }

    pub(crate) async fn publish_current_recipe(
        &self,
        mut plan: SourceAppliedCorporateActionPlan,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<SourceAppliedCorporateActionPlan, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        let coverage = plan
            .current_ordinary
            .as_mut()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
        if coverage.recipe.is_some()
            || coverage.references.is_empty()
            || coverage.references.len() > 16_000
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let recipe = CurrentRecipe {
            version: 1,
            source_binding: plan.source.binding_digest(),
            knowledge_cutoff: plan.plan.knowledge_cutoff(),
            valuation_cutoff: plan.plan.valuation_cutoff(),
            coverage_digest: coverage.digest,
            reads: std::mem::take(&mut coverage.references),
        };
        let bytes =
            serde_json::to_vec(&recipe).map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        if bytes.len() > MAX_CURRENT_RECIPE_BYTES {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let publication = ArtifactPublication::try_json(bytes)
            .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        let artifact = self
            .artifacts
            .as_ref()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?
            .publish(
                publication.clone(),
                ArtifactPublicationContext::new(cancellation.clone(), deadline),
            )
            .await
            .map_err(map_current_recipe_artifact_error)?;
        if !artifact.matches(&publication) {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        coverage.recipe = Some(CurrentOrdinaryRecipeReference::from_artifact(&artifact));
        check(deadline, cancellation)?;
        Ok(plan)
    }

    pub(super) async fn reopen_current_recipe(
        &self,
        reference: &CurrentOrdinaryRecipeReference,
        wire: &ApplicablePlanReferenceWire,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Vec<CurrentOrdinaryActionSourceRead>, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        if !reference.valid() {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let request = ArtifactReadRequest::try_new(
            reference.artifact()?,
            std::num::NonZeroUsize::new(MAX_CURRENT_RECIPE_BYTES)
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?,
        )
        .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        let read = self
            .artifacts
            .as_ref()
            .ok_or(ApplicableActionPlanError::InvalidEvidence)?
            .read(
                request,
                ArtifactReadContext::new(cancellation.clone(), deadline),
            )
            .await
            .map_err(map_current_recipe_artifact_error)?;
        if read.reference() != &reference.artifact()? {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let recipe: CurrentRecipe = serde_json::from_slice(read.content())
            .map_err(|_| ApplicableActionPlanError::InvalidEvidence)?;
        if recipe.version != 1
            || recipe.source_binding != wire.source_binding
            || recipe.knowledge_cutoff != wire.knowledge_cutoff
            || recipe.valuation_cutoff != wire.valuation_cutoff
            || Some(recipe.coverage_digest) != wire.current_ordinary_digest
            || recipe.reads.is_empty()
            || recipe.reads.len() > 16_000
            || recipe.reads.iter().any(|r| !r.valid())
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let reads = self
            .reopen_current_ordinary(
                &recipe.reads,
                wire.knowledge_cutoff,
                deadline,
                cancellation,
                job,
            )
            .await?;
        check(deadline, cancellation)?;
        Ok(reads)
    }

    pub(super) async fn reopen_current_ordinary(
        &self,
        references: &[CurrentOrdinaryReference],
        cutoff: Timestamp,
        deadline: Instant,
        cancellation: &CancellationToken,
        job: Option<&market_squawk_jobs::JobRunContext>,
    ) -> Result<Vec<CurrentOrdinaryActionSourceRead>, ApplicableActionPlanError> {
        if references.is_empty() || references.len() > 16_000 {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        let mut reads = Vec::new();
        reads
            .try_reserve_exact(references.len())
            .map_err(|_| ApplicableActionPlanError::SourceRead(ServiceError::ResourceExhausted))?;
        let mut total_audit = 0_usize;
        for reference in references {
            check(deadline, cancellation)?;
            if !reference.valid() {
                return Err(ApplicableActionPlanError::InvalidEvidence);
            }
            let manifest = self
                .research
                .analytical_reader()
                .provider_capture_origin(
                    reference.binding,
                    market_squawk_data::Sha256Digest::new(reference.origin_content.bytes()),
                    cutoff,
                    deadline,
                    cancellation,
                )
                .map_err(|error| ApplicableActionPlanError::SourceRead(map_analytical_error(error)))?
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
            let generation = self
                .research
                .read_provider_capture_generation_with_job_context(
                    job,
                    manifest,
                    deadline,
                    cancellation,
                    |generation, _, _, _, _| Ok(generation.clone()),
                )
                .await
                .map_err(|error| ApplicableActionPlanError::SourceRead(map_research_error(error)))?;
            let read = self
                .research
                .analytical_reader()
                .read_current_ordinary_source(
                    &generation,
                    &self.research.market_data_instruments(),
                    reference.subject,
                    cutoff,
                    deadline,
                    cancellation.clone(),
                )
                .await
                .map_err(|error| map_source_read_error(error, deadline, cancellation))?;
            if CurrentOrdinaryReference::from_read(&read) != *reference {
                return Err(ApplicableActionPlanError::InvalidEvidence);
            }
            total_audit = total_audit
                .checked_add(
                    read.retained_bytes()
                        .ok_or(ApplicableActionPlanError::InvalidEvidence)?,
                )
                .ok_or(ApplicableActionPlanError::InvalidEvidence)?;
            if total_audit > 64 * 1024 * 1024 {
                return Err(ApplicableActionPlanError::SourceRead(ServiceError::ResourceExhausted));
            }
            reads.push(read);
        }
        Ok(reads)
    }
}

fn map_current_recipe_artifact_error(error: market_squawk_services::ArtifactError) -> ApplicableActionPlanError {
    use market_squawk_services::ArtifactError as E;
    ApplicableActionPlanError::SourceRead(match error {
        E::Cancelled => ServiceError::Cancelled,
        E::DeadlineExceeded => ServiceError::DeadlineExceeded,
        E::ReadLimitExceeded => ServiceError::ResourceExhausted,
        E::NotFound => ServiceError::NotFound,
        E::Unavailable => ServiceError::Unavailable,
        E::InvalidPublication | E::InvalidReference => ServiceError::InvalidResult,
    })
}

impl SourceAppliedCorporateActionPlan {
    /// Aligns original forecast share units to the actual selected market event. The source
    /// recipe remains separately retained; decoding it must reopen this owner before reuse.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn current_forecast_share_conversion(
        &self,
        epoch: &market_squawk_data::FeatureDatasetInputEpoch,
        history: &market_squawk_data::ForecastBasisHistory,
        original_plan: &CorporateActionPlan,
        market: &crate::application::market_selection::MarketInvestmentReadReceipt,
        output_scale: u32,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_data::ForecastCurrentShareConversion, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        // Live generation always samples physical time; callers cannot supply an earlier clock.
        let admitted_at = current_share_wall_time()?;
        self.forecast_share_conversion_at_admission(epoch, history, original_plan, market,
            admitted_at, output_scale, deadline, cancellation)
    }

    /// Shared exact source reconstruction. The retained reader below is the only historical
    /// caller; current generation always supplies its own physical admission clock above.
    #[allow(clippy::too_many_arguments)]
    fn forecast_share_conversion_at_admission(
        &self,
        epoch: &market_squawk_data::FeatureDatasetInputEpoch,
        history: &market_squawk_data::ForecastBasisHistory,
        original_plan: &CorporateActionPlan,
        market: &crate::application::market_selection::MarketInvestmentReadReceipt,
        admitted_at: Timestamp,
        output_scale: u32,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<market_squawk_data::ForecastCurrentShareConversion, ApplicableActionPlanError> {
        check(deadline, cancellation)?;
        let invalid = || ApplicableActionPlanError::InvalidEvidence;
        // This still requires a currently authorized LocalAnalysis receipt even for old quotes.
        let observation = market.observation().map_err(|_| invalid())?;
        let mark = observation.mark();
        let quote_at = observation.timestamps().effective_at();
        let now = current_share_wall_time()?;
        if self.current_ordinary.as_ref().is_none_or(|coverage| coverage.recipe.is_none())
            || self.plan.source_split_admission().is_none()
            || self.plan.valuation_cutoff() != quote_at
            || admitted_at > now
            || self.plan.knowledge_cutoff() > admitted_at
            || self.plan.knowledge_cutoff() < observation.selected_at()
            || market.instrument_id() != epoch.instrument_id()
            || market.currency() != history.origin_price().currency()
            || mark.currency() != history.origin_price().currency()
            || mark.fresh_until().is_none_or(|expiry| admitted_at > expiry)
            || now >= market.authorization_expires_at()
        { return Err(invalid()); }
        let policy = CorporateActionPolicy::new(market_squawk_data::CorporateActionAdjustment::SplitAdjusted,
            std::num::NonZeroU32::MIN);
        let limits = self.plan.source_split_projection_limits(policy, epoch.instrument_id(),
            self.plan.knowledge_cutoff(), quote_at)
            .map_err(|error| map_source_plan_error(error, deadline, cancellation))?;
        let current_plan = self.plan.try_project_source_split_plan(policy, epoch.instrument_id(),
            self.plan.knowledge_cutoff(), quote_at, limits)
            .map_err(|error| map_source_plan_error(error, deadline, cancellation))?;
        let result = history.convert_to_current_share_units(epoch, original_plan, &current_plan,
            market.publication(), market.market_definitions(), output_scale, deadline, cancellation)
            .map_err(|error| match error {
                market_squawk_data::DatasetBuildError::Cancelled => ApplicableActionPlanError::SourceRead(ServiceError::Cancelled),
                market_squawk_data::DatasetBuildError::DeadlineExceeded => ApplicableActionPlanError::SourceRead(ServiceError::DeadlineExceeded),
                market_squawk_data::DatasetBuildError::LimitExceeded => ApplicableActionPlanError::SourceRead(ServiceError::ResourceExhausted),
                _ => invalid(),
            })?;
        if result.quote_at() != quote_at { return Err(invalid()); }
        check(deadline, cancellation)?;
        Ok(result)
    }
}

impl SourceAppliedCorporateActionReadCapability {
    /// Reopens the exact original current-action recipe; never selects latest action state.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_current_forecast_share_conversion(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        epoch: &market_squawk_data::FeatureDatasetInputEpoch,
        history: &market_squawk_data::ForecastBasisHistory,
        original_plan: &CorporateActionPlan,
        market: &crate::application::market_selection::MarketInvestmentReadReceipt,
        output_scale: u32,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<market_squawk_data::ForecastCurrentShareConversion>, ApplicableActionPlanError> {
        let Some(source) = self.read_reference(reference, deadline, cancellation.clone()).await? else {
            return Ok(None);
        };
        source.current_forecast_share_conversion(epoch, history, original_plan, market,
            output_scale, deadline, &cancellation).map(Some)
    }
}

impl SourceAppliedCorporateActionReadCapability {
    /// Reconstructs a saved conversion for historical display at its original admission.
    ///
    /// The caller must reopen the exact original epoch/history/plan and market references, and
    /// bind every original admission/authorization timestamp to the immutable saved proposal.
    /// The conversion identity binds the source coordinates, not those timestamps: the codec
    /// must also reconstruct and compare the saved valuation and decision projection identities.
    /// No result here grants current recommendation, paper, or execution authority.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn read_retained_forecast_share_conversion(
        &self,
        reference: &SourceAppliedCorporateActionPlanReference,
        epoch: &market_squawk_data::FeatureDatasetInputEpoch,
        history: &market_squawk_data::ForecastBasisHistory,
        original_plan: &CorporateActionPlan,
        market: &crate::application::market_selection::MarketInvestmentReadReceipt,
        original_admitted_at: Timestamp,
        original_authorized_at: Timestamp,
        original_authorization_expires_at: Timestamp,
        expected_conversion_identity: market_squawk_data::Sha256Digest,
        output_scale: u32,
        deadline: Instant,
        cancellation: CancellationToken,
    ) -> Result<Option<market_squawk_data::ForecastCurrentShareConversion>, ApplicableActionPlanError> {
        check(deadline, &cancellation)?;
        if original_authorized_at.unix_nanos() <= 0
            || original_authorized_at > original_admitted_at
            || original_admitted_at >= original_authorization_expires_at
            || expected_conversion_identity.bytes() == [0; 32]
        {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        // Physically reopen the original current-action capture, ordinary reads and calendar;
        // reference equality is enforced by the existing source owner, never a latest selector.
        let Some(source) = self.read_reference(reference, deadline, cancellation.clone()).await? else {
            return Ok(None);
        };
        let conversion = source.forecast_share_conversion_at_admission(epoch, history,
            original_plan, market, original_admitted_at, output_scale, deadline, &cancellation)?;
        if conversion.identity() != expected_conversion_identity {
            return Err(ApplicableActionPlanError::InvalidEvidence);
        }
        // Historical freshness never waives present rights to read the retained source.
        if current_share_wall_time()? >= market.authorization_expires_at() {
            return Err(ApplicableActionPlanError::SourceRead(ServiceError::Unauthorized));
        }
        check(deadline, &cancellation)?;
        Ok(Some(conversion))
    }
}

fn current_share_wall_time() -> Result<Timestamp, ApplicableActionPlanError> {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
        .ok().and_then(|value| i64::try_from(value.as_nanos()).ok())
        .map(Timestamp::from_unix_nanos)
        .ok_or(ApplicableActionPlanError::InvalidEvidence)
}
