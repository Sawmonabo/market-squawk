//! Bounded immutable fiscal pages and their complete original-source root.
use super::*;
use market_squawk_services::{
    ArtifactError, ArtifactPublication, ArtifactPublicationContext, ArtifactReadContext,
    ArtifactReadRequest, ArtifactReference,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroUsize,
};

pub(crate) const HISTORICAL_FISCAL_PAGE_SIZE: usize = 128;
pub(crate) const HISTORICAL_FISCAL_MAXIMUM_ORIGINS: usize =
    super::super::super::MAXIMUM_OBSERVATIONS_PER_GENERATION;
pub(crate) const HISTORICAL_FISCAL_MAXIMUM_PAGES: usize =
    HISTORICAL_FISCAL_MAXIMUM_ORIGINS.div_ceil(HISTORICAL_FISCAL_PAGE_SIZE);
const MAXIMUM_NATIVE_RECIPE_BYTES: usize = 2048;
const MAXIMUM_JOB_BINDING_BYTES: usize = 320;
// Existing artifact ceiling is unchanged. Pages have a smaller independently enforced bound.
const MAXIMUM_FISCAL_RECIPE_BYTES: usize = 2048 + 256 * (512 + 9 * (2048 + 1));
pub(crate) const HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES: usize =
    2048 + HISTORICAL_FISCAL_PAGE_SIZE * (512 + 9 * (2048 + 1 + 320));
const MAXIMUM_ROOT_BYTES: usize = 2048 + HISTORICAL_FISCAL_MAXIMUM_PAGES * 1024;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HistoricalFiscalJobReference {
    pub(crate) job_id: String,
    pub(crate) generation: u64,
}
impl HistoricalFiscalJobReference {
    fn validate(&self) -> Result<(), ServiceError> {
        let id = uuid::Uuid::parse_str(&self.job_id).map_err(|_| ServiceError::InvalidRequest)?;
        if id.hyphenated().to_string() != self.job_id || self.generation == 0 {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoricalFiscalRecipeReference {
    artifact_id: String,
    sha256: String,
    byte_count: usize,
}
impl HistoricalFiscalRecipeReference {
    pub(crate) fn artifact(&self) -> Result<ArtifactReference, ServiceError> {
        if self.byte_count == 0 || self.byte_count > MAXIMUM_FISCAL_RECIPE_BYTES {
            return Err(ServiceError::InvalidResult);
        }
        ArtifactReference::try_new(
            self.artifact_id.as_str(),
            self.sha256.as_str(),
            self.byte_count,
            "application/json",
        )
        .map_err(artifact_error)
    }
    pub(crate) fn identity(&self) -> Result<Sha256Digest, ServiceError> {
        self.artifact()?;
        Ok(Sha256Digest::new(
            decode_sha256(&self.sha256).ok_or(ServiceError::InvalidResult)?,
        ))
    }
}

/// Original jobs accompany native source recipes; neither these locators nor JSON grant authority.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HistoricalFiscalCompletedJobs {
    pub(crate) training_dataset: HistoricalFiscalJobReference,
    pub(crate) input_dataset: HistoricalFiscalJobReference,
    pub(crate) training: HistoricalFiscalJobReference,
}
impl HistoricalFiscalCompletedJobs {
    fn validate(&self) -> Result<(), ServiceError> {
        self.training_dataset.validate()?;
        self.input_dataset.validate()?;
        self.training.validate()?;
        bounded(self, MAXIMUM_JOB_BINDING_BYTES)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HistoricalFiscalOriginDescriptor {
    pub(crate) price_example_id: String,
    pub(crate) epoch_identity: [u8; 32],
    pub(crate) economic_origin_unix_nanos: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HistoricalFiscalStudyBinding {
    plan_identity: [u8; 32],
    profile_identity: [u8; 32],
    subject: market_squawk_domain::InstrumentId,
    source_cutoff: Timestamp,
    evaluation_start: Timestamp,
    evaluation_end: Timestamp,
    price_inputs: DatasetReference,
    study_input_job: HistoricalFiscalJobReference,
    total_origins: usize,
    epoch_set_digest: [u8; 32],
}
impl HistoricalFiscalStudyBinding {
    pub(crate) fn from_source(
        plan: &crate::application::analysis::HistoricalStudyPlanV1,
        prices: &FeatureDatasetInputEpochCursor,
        job: HistoricalFiscalJobReference,
    ) -> Result<Self, ServiceError> {
        let mut result = Self {
            plan_identity: Sha256::digest(
                serde_json::to_vec(plan.reference()).map_err(|_| ServiceError::InvalidResult)?,
            )
            .into(),
            profile_identity: Sha256::digest(
                serde_json::to_vec(plan.profile().resolution())
                    .map_err(|_| ServiceError::InvalidResult)?,
            )
            .into(),
            subject: plan.reference().subject_instrument_id(),
            source_cutoff: plan.reference().source_cutoff()?,
            evaluation_start: plan.folds()[0].starts_at(),
            evaluation_end: plan.folds()[2].ends_at(),
            price_inputs: DatasetReference::from_source(prices.dataset())?,
            study_input_job: job,
            total_origins: 0,
            epoch_set_digest: [0; 32],
        };
        let origins = result.derive_origins(prices)?;
        result.total_origins = origins.len();
        result.epoch_set_digest = origin_digest(&origins)?;
        result.validate()?;
        Ok(result)
    }
    fn validate(&self) -> Result<(), ServiceError> {
        self.study_input_job.validate()?;
        if self.plan_identity == [0; 32]
            || self.profile_identity == [0; 32]
            || self.epoch_set_digest == [0; 32]
            || self.total_origins == 0
            || self.total_origins > HISTORICAL_FISCAL_MAXIMUM_ORIGINS
            || self.evaluation_start >= self.evaluation_end
            || self.evaluation_end > self.source_cutoff
        {
            return Err(ServiceError::InvalidResult);
        }
        bounded(self, 1900)?;
        Ok(())
    }
    fn derive_origins(
        &self,
        prices: &FeatureDatasetInputEpochCursor,
    ) -> Result<Vec<HistoricalFiscalOriginDescriptor>, ServiceError> {
        if DatasetReference::from_source(prices.dataset())? != self.price_inputs {
            return Err(ServiceError::InvalidResult);
        }
        let mut examples = BTreeSet::new();
        let mut origins = Vec::new();
        let mut source_count = 0usize;
        for coordinate in prices.coordinates() {
            let coordinate = coordinate.map_err(super::super::super::super::map_read_error)?;
            let epoch = coordinate.epoch();
            if epoch.instrument_id() != self.subject {
                continue;
            }
            source_count = source_count
                .checked_add(1)
                .ok_or(ServiceError::ResourceExhausted)?;
            if source_count > HISTORICAL_FISCAL_MAXIMUM_ORIGINS {
                return Err(ServiceError::ResourceExhausted);
            }
            if epoch.source_selection_as_of() != self.source_cutoff {
                return Err(ServiceError::InvalidResult);
            }
            let at = epoch.decision_at().ok_or(ServiceError::InvalidResult)?;
            if at < self.evaluation_start || at >= self.evaluation_end {
                continue;
            }
            let example = epoch.example_id();
            if !examples.insert(example.to_owned())
                || example.is_empty()
                || example.len() > 256
                || !example.is_ascii()
            {
                return Err(ServiceError::InvalidResult);
            }
            origins.push(HistoricalFiscalOriginDescriptor {
                price_example_id: example.to_owned(),
                epoch_identity: Sha256::digest(
                    epoch.canonical_bytes().map_err(map_fiscal_build_error)?,
                )
                .into(),
                economic_origin_unix_nanos: epoch
                    .target_origin()
                    .ok_or(ServiceError::InvalidResult)?
                    .unix_nanos()
                    .to_string(),
            });
        }
        if origins.is_empty() {
            return Err(ServiceError::Unavailable);
        }
        origins.sort_by_key(|origin| origin.epoch_identity);
        if origins
            .windows(2)
            .any(|pair| pair[0].epoch_identity == pair[1].epoch_identity)
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(origins)
    }
    fn checked_origins(
        &self,
        prices: &FeatureDatasetInputEpochCursor,
    ) -> Result<Vec<HistoricalFiscalOriginDescriptor>, ServiceError> {
        self.validate()?;
        let origins = self.derive_origins(prices)?;
        if origins.len() != self.total_origins || origin_digest(&origins)? != self.epoch_set_digest
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(origins)
    }
    pub(crate) fn page(
        &self,
        prices: &FeatureDatasetInputEpochCursor,
        ordinal: usize,
    ) -> Result<HistoricalFiscalPageDescriptor, ServiceError> {
        let origins = self.checked_origins(prices)?;
        let chunk = origins
            .chunks(HISTORICAL_FISCAL_PAGE_SIZE)
            .nth(ordinal)
            .ok_or(ServiceError::InvalidRequest)?;
        Ok(HistoricalFiscalPageDescriptor {
            binding: self.clone(),
            page_ordinal: ordinal,
            total_origins: self.total_origins,
            total_pages: self.total_origins.div_ceil(HISTORICAL_FISCAL_PAGE_SIZE),
            epoch_set_digest: self.epoch_set_digest,
            origins: chunk.to_vec(),
        })
    }
    pub(crate) fn contains(
        &self,
        prices: &FeatureDatasetInputEpochCursor,
        example: &str,
    ) -> Result<(), ServiceError> {
        if !self
            .checked_origins(prices)?
            .iter()
            .any(|origin| origin.price_example_id == example)
        {
            return Err(ServiceError::InvalidRequest);
        }
        Ok(())
    }
}
fn origin_digest(origins: &[HistoricalFiscalOriginDescriptor]) -> Result<[u8; 32], ServiceError> {
    let mut digest = Sha256::new();
    digest.update(b"market-squawk/historical-fiscal-origin-set/v1\0");
    for origin in origins {
        digest.update(serde_json::to_vec(origin).map_err(|_| ServiceError::InvalidResult)?);
    }
    Ok(digest.finalize().into())
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HistoricalFiscalPageDescriptor {
    pub(crate) binding: HistoricalFiscalStudyBinding,
    pub(crate) page_ordinal: usize,
    pub(crate) total_origins: usize,
    pub(crate) total_pages: usize,
    pub(crate) epoch_set_digest: [u8; 32],
    pub(crate) origins: Vec<HistoricalFiscalOriginDescriptor>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct HistoricalFiscalPageReference {
    binding_digest: [u8; 32],
    page_ordinal: usize,
    origin_count: usize,
    artifact: HistoricalFiscalRecipeReference,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct NativeRecipe {
    inputs: DatasetReference,
    runtime: ForecastStudyRuntimeReference,
    distribution: [u8; 32],
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ReadyRecipe {
    native: NativeRecipe,
    jobs: HistoricalFiscalCompletedJobs,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct OriginRecipe {
    example: String,
    identity: [u8; 32],
    targets: [Option<ReadyRecipe>; 9],
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Page {
    version: u8,
    target_ids: [String; 9],
    binding_digest: [u8; 32],
    ordinal: usize,
    origins: Vec<OriginRecipe>,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Root {
    version: u8,
    binding: HistoricalFiscalStudyBinding,
    pages: Vec<HistoricalFiscalPageReference>,
}
/// Keeps only the root/index; a use opens at most one immutable page and one origin's inference.
pub(crate) struct HistoricalFiscalSourceSelection {
    reference: HistoricalFiscalRecipeReference,
    root: Root,
}
impl HistoricalFiscalSourceSelection {
    pub(crate) fn reference(&self) -> &HistoricalFiscalRecipeReference {
        &self.reference
    }
    pub(crate) fn artifacts(&self) -> Result<Vec<ArtifactReference>, ServiceError> {
        std::iter::once(self.reference.artifact())
            .chain(self.root.pages.iter().map(|p| p.artifact.artifact()))
            .collect()
    }
}
fn binding_digest(binding: &HistoricalFiscalStudyBinding) -> Result<[u8; 32], ServiceError> {
    Ok(
        Sha256::digest(serde_json::to_vec(binding).map_err(|_| ServiceError::InvalidResult)?)
            .into(),
    )
}
fn bounded(value: &impl Serialize, maximum: usize) -> Result<Vec<u8>, ServiceError> {
    let bytes = serde_json::to_vec(value).map_err(|_| ServiceError::InvalidResult)?;
    if bytes.len() > maximum {
        return Err(ServiceError::ResourceExhausted);
    }
    Ok(bytes)
}
impl Page {
    fn from_sources(
        descriptor: &HistoricalFiscalPageDescriptor,
        forecasts: Vec<(
            HistoricalFiscalForecastReference,
            HistoricalFiscalCompletedJobs,
        )>,
        unavailable: Vec<HistoricalFiscalUnavailableReference>,
    ) -> Result<Self, ServiceError> {
        if forecasts.len().checked_add(unavailable.len()) != Some(descriptor.origins.len() * 9) {
            return Err(ServiceError::InvalidRequest);
        }
        let targets = crate::application::research::fiscal_projection::fiscal_projection_targets();
        let mut origins = BTreeMap::<[u8; 32], (OriginRecipe, [bool; 9])>::new();
        let successes = forecasts.into_iter().map(|(s, jobs)| {
            (
                s.price_inputs,
                s.price_example_id,
                s.price_origin_identity,
                s.target_id,
                Some(ReadyRecipe {
                    native: NativeRecipe {
                        inputs: s.financial_inputs,
                        runtime: s.runtime,
                        distribution: s.distribution_identity,
                    },
                    jobs,
                }),
            )
        });
        let failures = unavailable.into_iter().map(|s| {
            (
                s.price_inputs,
                s.price_example_id,
                s.price_origin_identity,
                s.target_id,
                None,
            )
        });
        for (prices, example, identity, target, recipe) in successes.chain(failures) {
            if prices != descriptor.binding.price_inputs {
                return Err(ServiceError::InvalidResult);
            }
            let index = targets
                .iter()
                .position(|t| t.target_id == target)
                .ok_or(ServiceError::InvalidRequest)?;
            let (origin, seen) = origins.entry(identity).or_insert_with(|| {
                (
                    OriginRecipe {
                        example: example.clone(),
                        identity,
                        targets: std::array::from_fn(|_| None),
                    },
                    [false; 9],
                )
            });
            if origin.example != example || seen[index] {
                return Err(ServiceError::InvalidRequest);
            }
            seen[index] = true;
            origin.targets[index] = recipe;
        }
        if origins.values().any(|(_, seen)| seen.iter().any(|s| !*s)) {
            return Err(ServiceError::InvalidRequest);
        }
        let page = Self {
            version: 1,
            target_ids: targets
                .into_iter()
                .map(|target| target.target_id)
                .collect::<Vec<_>>()
                .try_into()
                .map_err(|_| ServiceError::InvalidResult)?,
            binding_digest: binding_digest(&descriptor.binding)?,
            ordinal: descriptor.page_ordinal,
            origins: origins.into_values().map(|(o, _)| o).collect(),
        };
        page.validate(descriptor)?;
        Ok(page)
    }
    fn validate(&self, expected: &HistoricalFiscalPageDescriptor) -> Result<(), ServiceError> {
        let current_targets =
            crate::application::research::fiscal_projection::fiscal_projection_targets();
        if current_targets.len() != 9
            || self
                .target_ids
                .iter()
                .zip(&current_targets)
                .any(|(original, current)| original != &current.target_id)
        {
            return Err(ServiceError::InvalidResult);
        }
        if self.version != 1
            || self.binding_digest != binding_digest(&expected.binding)?
            || self.ordinal != expected.page_ordinal
            || self.origins.len() != expected.origins.len()
            || self.origins.is_empty()
            || self.origins.len() > HISTORICAL_FISCAL_PAGE_SIZE
        {
            return Err(ServiceError::InvalidResult);
        }
        for (origin, expected) in self.origins.iter().zip(&expected.origins) {
            if origin.example != expected.price_example_id
                || origin.identity != expected.epoch_identity
            {
                return Err(ServiceError::InvalidResult);
            }
            bounded(&(&origin.example, &origin.identity), 448)?;
            for ready in origin.targets.iter().flatten() {
                if ready.native.distribution == [0; 32] {
                    return Err(ServiceError::InvalidResult);
                }
                bounded(&ready.native, MAXIMUM_NATIVE_RECIPE_BYTES)?;
                ready.jobs.validate()?;
                // Include JSON field/wrapper overhead within the proven per-slot envelope.
                bounded(
                    ready,
                    MAXIMUM_NATIVE_RECIPE_BYTES + MAXIMUM_JOB_BINDING_BYTES,
                )?;
            }
        }
        bounded(self, HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES)?;
        Ok(())
    }
}
impl Root {
    fn validate(&self) -> Result<(), ServiceError> {
        self.binding.validate()?;
        if self.version != 1
            || self.pages.len()
                != self
                    .binding
                    .total_origins
                    .div_ceil(HISTORICAL_FISCAL_PAGE_SIZE)
            || self.pages.len() > HISTORICAL_FISCAL_MAXIMUM_PAGES
        {
            return Err(ServiceError::InvalidResult);
        }
        let digest = binding_digest(&self.binding)?;
        let mut identities = BTreeSet::new();
        for (ordinal, page) in self.pages.iter().enumerate() {
            let count = (self.binding.total_origins - ordinal * HISTORICAL_FISCAL_PAGE_SIZE)
                .min(HISTORICAL_FISCAL_PAGE_SIZE);
            if page.page_ordinal != ordinal
                || page.origin_count != count
                || page.binding_digest != digest
                || !identities.insert(page.artifact.sha256.as_str())
                || page.artifact.byte_count > HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES
            {
                return Err(ServiceError::InvalidResult);
            }
            page.artifact.artifact()?;
            bounded(page, 1024)?;
        }
        bounded(self, MAXIMUM_ROOT_BYTES)?;
        Ok(())
    }
}
impl HistoricalFiscalForecastReadCapability {
    async fn publish_recipe(
        &self,
        bytes: Vec<u8>,
        context: &RequestContext,
    ) -> Result<HistoricalFiscalRecipeReference, ServiceError> {
        ensure_request(context)?;
        if bytes.len() > MAXIMUM_FISCAL_RECIPE_BYTES {
            return Err(ServiceError::ResourceExhausted);
        }
        let publication = ArtifactPublication::try_json(bytes).map_err(artifact_error)?;
        let sha256 = publication.sha256_hex().to_owned();
        let byte_count = publication.byte_count();
        let artifact = self
            .artifacts
            .publish(
                publication,
                ArtifactPublicationContext::new(context.cancellation().clone(), context.deadline()),
            )
            .await
            .map_err(artifact_error)?;
        ensure_request(context)?;
        if artifact.sha256() != sha256
            || artifact.byte_count() != byte_count
            || artifact.media_type() != "application/json"
        {
            return Err(ServiceError::InvalidResult);
        }
        Ok(HistoricalFiscalRecipeReference {
            artifact_id: artifact.id().to_owned(),
            sha256,
            byte_count,
        })
    }
    async fn read_recipe<T: serde::de::DeserializeOwned>(
        &self,
        reference: &HistoricalFiscalRecipeReference,
        maximum: usize,
        context: &RequestContext,
    ) -> Result<T, ServiceError> {
        ensure_request(context)?;
        if reference.byte_count > maximum {
            return Err(ServiceError::ResourceExhausted);
        }
        let exact = reference.artifact()?;
        let read = self
            .artifacts
            .read(
                ArtifactReadRequest::try_new(
                    exact.clone(),
                    NonZeroUsize::new(maximum).ok_or(ServiceError::Internal)?,
                )
                .map_err(artifact_error)?,
                ArtifactReadContext::new(context.cancellation().clone(), context.deadline()),
            )
            .await
            .map_err(artifact_error)?;
        if read.reference() != &exact {
            return Err(ServiceError::InvalidResult);
        }
        serde_json::from_slice(read.content()).map_err(|_| ServiceError::InvalidResult)
    }
    pub(crate) async fn publish_page(
        &self,
        descriptor: &HistoricalFiscalPageDescriptor,
        forecasts: Vec<(
            HistoricalFiscalForecastReference,
            HistoricalFiscalCompletedJobs,
        )>,
        unavailable: Vec<HistoricalFiscalUnavailableReference>,
        context: &RequestContext,
    ) -> Result<HistoricalFiscalPageReference, ServiceError> {
        let prices = descriptor
            .binding
            .price_inputs
            .read(
                &self.research,
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                context,
            )
            .await?;
        let original = descriptor.binding.page(&prices, descriptor.page_ordinal)?;
        if &original != descriptor {
            return Err(ServiceError::InvalidResult);
        }
        let page = Page::from_sources(&original, forecasts, unavailable)?;
        let artifact = self
            .publish_recipe(
                bounded(&page, HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES)?,
                context,
            )
            .await?;
        Ok(HistoricalFiscalPageReference {
            binding_digest: page.binding_digest,
            page_ordinal: page.ordinal,
            origin_count: page.origins.len(),
            artifact,
        })
    }
    /// Reopen acknowledged immutable custody without selecting models or republishing a page.
    /// Full financial/source inference is still repeated before the complete root can be admitted.
    pub(crate) async fn read_page_reference(
        &self,
        binding: &HistoricalFiscalStudyBinding,
        prices: &FeatureDatasetInputEpochCursor,
        ordinal: usize,
        reference: &HistoricalFiscalPageReference,
        context: &RequestContext,
    ) -> Result<HistoricalFiscalPageDescriptor, ServiceError> {
        ensure_request(context)?;
        let descriptor = binding.page(prices, ordinal)?;
        if reference.page_ordinal != ordinal
            || reference.binding_digest != binding_digest(binding)?
            || reference.origin_count != descriptor.origins.len()
        {
            return Err(ServiceError::InvalidRequest);
        }
        bounded(reference, 1024)?;
        let page: Page = self
            .read_recipe(
                &reference.artifact,
                HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES,
                context,
            )
            .await?;
        page.validate(&descriptor)?;
        ensure_request(context)?;
        Ok(descriptor)
    }
    pub(crate) async fn publish_selection(
        &self,
        binding: HistoricalFiscalStudyBinding,
        prices: &FeatureDatasetInputEpochCursor,
        pages: Vec<HistoricalFiscalPageReference>,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<HistoricalFiscalSourceSelection, ServiceError> {
        let root = Root {
            version: 1,
            binding,
            pages,
        };
        root.validate()?;
        self.validate_pages(&root, prices, profile, context).await?;
        let reference = self
            .publish_recipe(bounded(&root, MAXIMUM_ROOT_BYTES)?, context)
            .await?;
        Ok(HistoricalFiscalSourceSelection { reference, root })
    }
    pub(crate) async fn read_selection(
        &self,
        reference: &HistoricalFiscalRecipeReference,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<HistoricalFiscalSourceSelection, ServiceError> {
        let root: Root = self
            .read_recipe(reference, MAXIMUM_ROOT_BYTES, context)
            .await?;
        root.validate()?;
        let prices = root
            .binding
            .price_inputs
            .read(
                &self.research,
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                context,
            )
            .await?;
        self.validate_pages(&root, &prices, profile, context)
            .await?;
        Ok(HistoricalFiscalSourceSelection {
            reference: reference.clone(),
            root,
        })
    }
    /// Physical original-byte custody for the materialization/backup owner; never a forecast receipt.
    pub(crate) async fn recipe_artifacts(
        &self,
        reference: &HistoricalFiscalRecipeReference,
        context: &RequestContext,
    ) -> Result<Vec<ArtifactReference>, ServiceError> {
        let root: Root = self
            .read_recipe(reference, MAXIMUM_ROOT_BYTES, context)
            .await?;
        root.validate()?;
        let prices = root
            .binding
            .price_inputs
            .read(
                &self.research,
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                context,
            )
            .await?;
        for page in &root.pages {
            let descriptor = root.binding.page(&prices, page.page_ordinal)?;
            let actual: Page = self
                .read_recipe(
                    &page.artifact,
                    HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES,
                    context,
                )
                .await?;
            actual.validate(&descriptor)?;
        }
        HistoricalFiscalSourceSelection {
            reference: reference.clone(),
            root,
        }
        .artifacts()
    }
    async fn validate_pages(
        &self,
        root: &Root,
        prices: &FeatureDatasetInputEpochCursor,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<(), ServiceError> {
        let actual_profile: [u8; 32] = Sha256::digest(
            serde_json::to_vec(profile.resolution()).map_err(|_| ServiceError::InvalidResult)?,
        )
        .into();
        if actual_profile != root.binding.profile_identity {
            return Err(ServiceError::InvalidResult);
        }
        root.binding.checked_origins(prices)?;
        for page in &root.pages {
            ensure_request(context)?;
            let descriptor = root.binding.page(prices, page.page_ordinal)?;
            let actual: Page = self
                .read_recipe(
                    &page.artifact,
                    HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES,
                    context,
                )
                .await?;
            actual.validate(&descriptor)?;
            for origin in &actual.origins {
                drop(
                    self.read_origin_recipe(&root.binding, prices, origin, profile, context)
                        .await?,
                );
            }
        }
        Ok(())
    }
    pub(crate) async fn read_origin(
        &self,
        selection: &HistoricalFiscalSourceSelection,
        epoch: &market_squawk_data::FeatureDatasetInputEpoch,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<Vec<HistoricalOriginFinancialForecast>, ServiceError> {
        let profile_identity: [u8; 32] = Sha256::digest(
            serde_json::to_vec(profile.resolution()).map_err(|_| ServiceError::InvalidResult)?,
        )
        .into();
        if profile_identity != selection.root.binding.profile_identity {
            return Err(ServiceError::InvalidResult);
        }
        let identity: [u8; 32] =
            Sha256::digest(epoch.canonical_bytes().map_err(map_fiscal_build_error)?).into();
        let prices = selection
            .root
            .binding
            .price_inputs
            .read(
                &self.research,
                FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
                context,
            )
            .await?;
        let origins = selection.root.binding.checked_origins(&prices)?;
        let index = origins
            .binary_search_by_key(&identity, |origin| origin.epoch_identity)
            .map_err(|_| ServiceError::InvalidResult)?;
        let ordinal = index / HISTORICAL_FISCAL_PAGE_SIZE;
        let reference = selection
            .root
            .pages
            .get(ordinal)
            .ok_or(ServiceError::InvalidResult)?;
        let page: Page = self
            .read_recipe(
                &reference.artifact,
                HISTORICAL_FISCAL_MAXIMUM_PAGE_BYTES,
                context,
            )
            .await?;
        page.validate(&selection.root.binding.page(&prices, ordinal)?)?;
        let origin = page
            .origins
            .get(index % HISTORICAL_FISCAL_PAGE_SIZE)
            .ok_or(ServiceError::InvalidResult)?;
        self.read_origin_recipe(&selection.root.binding, &prices, origin, profile, context)
            .await
    }
    async fn read_origin_recipe(
        &self,
        binding: &HistoricalFiscalStudyBinding,
        prices: &FeatureDatasetInputEpochCursor,
        origin: &OriginRecipe,
        profile: &ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<Vec<HistoricalOriginFinancialForecast>, ServiceError> {
        ensure_request(context)?;
        let identity = origin.identity;
        let mut selected = None;
        for coordinate in prices.coordinates() {
            let coordinate = coordinate.map_err(super::super::super::super::map_read_error)?;
            if coordinate.epoch().example_id() == origin.example {
                if selected.replace(coordinate).is_some() {
                    return Err(ServiceError::InvalidResult);
                }
            }
        }
        let selected = selected.ok_or(ServiceError::Unavailable)?;
        let price = selected.coordinate();
        let epoch = price.epoch();
        if <[u8; 32]>::from(Sha256::digest(
            epoch.canonical_bytes().map_err(map_fiscal_build_error)?,
        )) != identity
        {
            return Err(ServiceError::InvalidResult);
        }
        let population = prepare_fixed_current_population(
            &self.research,
            Arc::clone(&self.identities),
            vec![epoch.instrument_id()],
            Sha256Digest::new(
                decode_sha256(&profile.resolution().configuration_digest)
                    .ok_or(ServiceError::InvalidRequest)?,
            ),
            epoch.source_selection_as_of(),
            context.deadline(),
            context.cancellation(),
        )
        .await?;
        let targets = crate::application::research::fiscal_projection::fiscal_projection_targets();
        let mut forecasts = Vec::with_capacity(9);
        for (target, ready) in targets.iter().zip(&origin.targets) {
            let native = ready.as_ref().map(|ready| &ready.native);
            ensure_request(context)?;
            let prepared = self
                .datasets
                .prepare_historical_financial_datasets(
                    epoch,
                    target,
                    population.clone(),
                    profile,
                    context,
                )
                .await;
            match (native, prepared) {
                (Some(native), Ok(prepared)) => {
                    let (_, _, _, expectation) = prepared.into_parts();
                    let financial=native.inputs.read(&self.research,FeatureDatasetProductContract::FinancialAmountFiscalPeriodsStudyInputsV1,context).await?;
                    let runtime = self
                        .runtime
                        .read_forecast_runtime_reference(&native.runtime)?;
                    let forecast = expectation.forecast(
                        &self.research,
                        &runtime,
                        &financial,
                        price,
                        context,
                    )?;
                    let expected = HistoricalFiscalForecastReference {
                        target_id: target.target_id.clone(),
                        price_inputs: binding.price_inputs.clone(),
                        price_example_id: origin.example.clone(),
                        price_origin_identity: origin.identity,
                        financial_inputs: native.inputs.clone(),
                        runtime: native.runtime.clone(),
                        distribution_identity: native.distribution,
                    };
                    if forecast.reference() != &expected {
                        return Err(ServiceError::InvalidResult);
                    }
                    forecasts.push(forecast);
                }
                (None, Err(ServiceError::Unavailable | ServiceError::NotFound)) => {}
                (_, Err(error)) => return Err(error),
                (None, Ok(_)) => return Err(ServiceError::InvalidResult),
            }
        }
        Ok(forecasts)
    }
}
fn artifact_error(error: ArtifactError) -> ServiceError {
    match error {
        ArtifactError::Cancelled => ServiceError::Cancelled,
        ArtifactError::DeadlineExceeded => ServiceError::DeadlineExceeded,
        ArtifactError::ReadLimitExceeded => ServiceError::ResourceExhausted,
        ArtifactError::NotFound => ServiceError::NotFound,
        ArtifactError::InvalidPublication | ArtifactError::InvalidReference => {
            ServiceError::InvalidResult
        }
        ArtifactError::Unavailable => ServiceError::Unavailable,
    }
}
