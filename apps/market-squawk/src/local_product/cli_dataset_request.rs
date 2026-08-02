//! Closed JSON DTO conversion into invariant-preserving dataset-build contracts.

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    str::FromStr as _,
    time::Duration,
};

use market_squawk_data::{
    ChronologicalSplitPolicy, ComponentAdjustmentEvidence, ComponentKind, ComponentScope,
    ComponentSelector, ComponentValue, CorporateActionAdjustment, CorporateActionLimits,
    CorporateActionPolicy, CorporateActionSensitivity, DatasetBuildInputs, DatasetBuildLimits,
    DatasetBuildPolicy, DatasetBuildRequest, DatasetExample, DatasetId, DatasetManifestRef,
    DatasetOutputAuthorization, DatasetSchemaRef, DatasetSchemaRegistry,
    FeatureLabelComponentInput, FeatureLabelComponentSpec, MissingValuePolicy,
    ObservationFamilyKey, PointInTimeLimits, PointInTimePolicy, PointInTimeRevisionMode,
    ResearchUse, ResearchUseLimits, RightsBasis, Sha256Digest, UniverseId, UniverseLimits,
    UniverseMembership,
};
use market_squawk_domain::{
    AvailabilityEvidence, CalendarDate, Currency, DigestAlgorithm, EffectiveInterval,
    EvidenceDigest, InstrumentId, ResearchPeriod, ResearchTemporalCoordinate, SchemaVersion,
    SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::UserOwnedInputEvidence;
use rust_decimal::Decimal;
use serde::Deserialize;

use super::CliDatasetError;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct DatasetBuildRequestDto {
    output_dataset: String,
    parents: Vec<ManifestDto>,
    universe: UniverseDto,
    component_specs: Vec<ComponentSpecDto>,
    examples: Vec<ExampleDto>,
    policy: PolicyDto,
    intended_use: ResearchUseDto,
    research_use_limits: ResearchUseLimitsDto,
    output_authorization: OutputAuthorizationDto,
    limits: BuildLimitsDto,
}

impl DatasetBuildRequestDto {
    pub(super) fn into_domain(
        self,
        ownership: UserOwnedInputEvidence,
    ) -> Result<DatasetBuildRequest, CliDatasetError> {
        let parents = convert_all(self.parents, ManifestDto::into_domain)?;
        let component_specs = convert_all(self.component_specs, ComponentSpecDto::into_domain)?;
        let examples = convert_all(self.examples, ExampleDto::into_domain)?;
        let inputs = DatasetBuildInputs::try_new(
            parents,
            UniverseId::try_from(self.universe.id).map_err(|_| CliDatasetError::InvalidRequest)?,
            convert_all(
                self.universe.memberships,
                UniverseMembershipDto::into_domain,
            )?,
            component_specs,
            examples,
        )
        .map_err(|_| CliDatasetError::InvalidRequest)?;
        DatasetBuildRequest::try_new(
            DatasetId::try_from(self.output_dataset.as_str())
                .map_err(|_| CliDatasetError::InvalidRequest)?,
            inputs,
            self.policy.into_domain()?,
            self.intended_use.into_domain(),
            self.research_use_limits.into_domain()?,
            self.output_authorization.into_domain(ownership)?,
            self.limits.into_domain()?,
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ManifestDto {
    dataset: String,
    version: u64,
    schema: String,
    schema_version: u16,
    schema_fingerprint_sha256: String,
    content_sha256: String,
}

impl ManifestDto {
    fn into_domain(self) -> Result<DatasetManifestRef, CliDatasetError> {
        let schema = DatasetSchemaRef::try_new(
            self.schema,
            SchemaVersion::new(self.schema_version).map_err(|_| CliDatasetError::InvalidRequest)?,
            decode_sha256(&self.schema_fingerprint_sha256)?,
        )
        .map_err(|_| CliDatasetError::InvalidRequest)?;
        DatasetSchemaRegistry::local()
            .resolve(&schema)
            .map_err(|_| CliDatasetError::InvalidRequest)?;
        DatasetManifestRef::try_new_with_schema(
            DatasetId::try_from(self.dataset.as_str())
                .map_err(|_| CliDatasetError::InvalidRequest)?,
            self.version,
            schema,
            Sha256Digest::new(decode_nonzero_sha256(&self.content_sha256)?),
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UniverseDto {
    id: String,
    memberships: Vec<UniverseMembershipDto>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UniverseMembershipDto {
    instrument_id: InstrumentId,
    starts_at_unix_nanos: i64,
    ends_at_unix_nanos: Option<i64>,
    availability: AvailabilityDto,
    source_manifest: ManifestDto,
    evidence_sha256: String,
}

impl UniverseMembershipDto {
    fn into_domain(self) -> Result<UniverseMembership, CliDatasetError> {
        Ok(UniverseMembership::new(
            self.instrument_id,
            EffectiveInterval::new(
                timestamp(self.starts_at_unix_nanos),
                self.ends_at_unix_nanos.map(timestamp),
            )
            .map_err(|_| CliDatasetError::InvalidRequest)?,
            self.availability.into_domain(),
            self.source_manifest.into_domain()?,
            evidence(&self.evidence_sha256)?,
        ))
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum AvailabilityDto {
    Evidenced {
        available_at_unix_nanos: i64,
        evidence: SourceIdentifier,
    },
    LocalFirstObserved {
        observed_at_unix_nanos: i64,
    },
    Inferred {
        inferred_at_unix_nanos: i64,
        method: SourceIdentifier,
    },
    Unknown,
}

impl AvailabilityDto {
    fn into_domain(self) -> AvailabilityEvidence {
        match self {
            Self::Evidenced {
                available_at_unix_nanos,
                evidence,
            } => AvailabilityEvidence::evidenced(timestamp(available_at_unix_nanos), evidence),
            Self::LocalFirstObserved {
                observed_at_unix_nanos,
            } => AvailabilityEvidence::local_first_observed(timestamp(observed_at_unix_nanos)),
            Self::Inferred {
                inferred_at_unix_nanos,
                method,
            } => AvailabilityEvidence::inferred(timestamp(inferred_at_unix_nanos), method),
            Self::Unknown => AvailabilityEvidence::unknown(),
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ComponentSpecDto {
    kind: ComponentKindDto,
    scope: ComponentScopeDto,
    corporate_actions: CorporateActionSensitivityDto,
    name: String,
    version: u32,
}

impl ComponentSpecDto {
    fn into_domain(self) -> Result<FeatureLabelComponentSpec, CliDatasetError> {
        FeatureLabelComponentSpec::try_new(
            self.kind.into_domain(),
            self.scope.into_domain(),
            self.corporate_actions.into_domain(),
            self.name,
            NonZeroU32::new(self.version).ok_or(CliDatasetError::InvalidRequest)?,
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ComponentKindDto {
    Feature,
    Label,
}

impl ComponentKindDto {
    const fn into_domain(self) -> ComponentKind {
        match self {
            Self::Feature => ComponentKind::Feature,
            Self::Label => ComponentKind::Label,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ComponentScopeDto {
    Instrument,
    Account,
    Global,
}

impl ComponentScopeDto {
    const fn into_domain(self) -> ComponentScope {
        match self {
            Self::Instrument => ComponentScope::Instrument,
            Self::Account => ComponentScope::Account,
            Self::Global => ComponentScope::Global,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CorporateActionSensitivityDto {
    NotApplicable,
    RequiresAdjustment,
}

impl CorporateActionSensitivityDto {
    const fn into_domain(self) -> CorporateActionSensitivity {
        match self {
            Self::NotApplicable => CorporateActionSensitivity::NotApplicable,
            Self::RequiresAdjustment => CorporateActionSensitivity::RequiresAdjustment,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ExampleDto {
    example_id: String,
    instrument_id: InstrumentId,
    cutoff_at_unix_nanos: i64,
    label_cutoff_at_unix_nanos: i64,
    components: Vec<ComponentInputDto>,
}

impl ExampleDto {
    fn into_domain(self) -> Result<DatasetExample, CliDatasetError> {
        DatasetExample::try_new(
            self.example_id,
            self.instrument_id,
            timestamp(self.cutoff_at_unix_nanos),
            timestamp(self.label_cutoff_at_unix_nanos),
            convert_all(self.components, ComponentInputDto::into_domain)?,
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ComponentInputDto {
    spec: ComponentSpecDto,
    value: ComponentValueDto,
    selectors: Vec<ObservationFamilyDto>,
    adjustment: AdjustmentEvidenceDto,
}

impl ComponentInputDto {
    fn into_domain(self) -> Result<FeatureLabelComponentInput, CliDatasetError> {
        FeatureLabelComponentInput::try_new(
            self.spec.into_domain()?,
            self.value.into_domain()?,
            self.selectors
                .into_iter()
                .map(ObservationFamilyDto::into_domain)
                .map(|result| result.map(ComponentSelector::new))
                .collect::<Result<Vec<_>, _>>()?,
            self.adjustment.into_domain()?,
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum ComponentValueDto {
    Float {
        value: f64,
        unit: Option<SourceIdentifier>,
        currency: Option<Currency>,
    },
    Decimal {
        value: String,
        unit: Option<SourceIdentifier>,
        currency: Option<Currency>,
    },
    Missing {
        reason: SourceIdentifier,
    },
}

impl ComponentValueDto {
    fn into_domain(self) -> Result<ComponentValue, CliDatasetError> {
        match self {
            Self::Float {
                value,
                unit,
                currency,
            } => ComponentValue::float(value, unit, currency),
            Self::Decimal {
                value,
                unit,
                currency,
            } => ComponentValue::decimal(
                Decimal::from_str(&value).map_err(|_| CliDatasetError::InvalidRequest)?,
                unit,
                currency,
            ),
            Self::Missing { reason } => Ok(ComponentValue::missing(reason)),
        }
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum AdjustmentEvidenceDto {
    Raw,
    NotApplicable,
    Applied {
        policy: CorporateActionPolicyDto,
        plan_content_sha256: String,
        plan_audit_sha256: String,
        implementation_sha256: String,
    },
}

impl AdjustmentEvidenceDto {
    fn into_domain(self) -> Result<ComponentAdjustmentEvidence, CliDatasetError> {
        match self {
            Self::Raw => Ok(ComponentAdjustmentEvidence::Raw),
            Self::NotApplicable => Ok(ComponentAdjustmentEvidence::NotApplicable),
            Self::Applied {
                policy,
                plan_content_sha256,
                plan_audit_sha256,
                implementation_sha256,
            } => ComponentAdjustmentEvidence::try_applied(
                policy.into_domain()?,
                Sha256Digest::new(decode_nonzero_sha256(&plan_content_sha256)?),
                Sha256Digest::new(decode_nonzero_sha256(&plan_audit_sha256)?),
                evidence(&implementation_sha256)?,
            )
            .map_err(|_| CliDatasetError::InvalidRequest),
        }
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum ObservationFamilyDto {
    Filing {
        source_id: SourceId,
        instrument_id: InstrumentId,
        accession: SourceIdentifier,
    },
    Fundamental {
        source_id: SourceId,
        instrument_id: InstrumentId,
        source_record: SourceIdentifier,
        concept: SourceIdentifier,
        unit: SourceIdentifier,
        effective: TemporalCoordinateDto,
    },
    Macro {
        source_id: SourceId,
        series: SourceIdentifier,
        effective: TemporalCoordinateDto,
    },
    PortfolioPosition {
        source_id: SourceId,
        instrument_id: InstrumentId,
        account_id: SourceIdentifier,
        effective: TemporalCoordinateDto,
    },
    Transaction {
        source_id: SourceId,
        instrument_id: Option<InstrumentId>,
        account_id: SourceIdentifier,
        source_record_id: SourceIdentifier,
    },
    CorporateAction {
        source_id: SourceId,
        instrument_id: InstrumentId,
        source_record: SourceIdentifier,
    },
    UniverseMembership {
        source_id: SourceId,
        instrument_id: InstrumentId,
        source_record: SourceIdentifier,
        universe: SourceIdentifier,
    },
    AlternativeData {
        source_id: SourceId,
        instrument_id: Option<InstrumentId>,
        source_record: SourceIdentifier,
        dataset: SourceIdentifier,
        field: SourceIdentifier,
        effective: TemporalCoordinateDto,
    },
}

impl ObservationFamilyDto {
    fn into_domain(self) -> Result<ObservationFamilyKey, CliDatasetError> {
        Ok(match self {
            Self::Filing {
                source_id,
                instrument_id,
                accession,
            } => ObservationFamilyKey::Filing {
                source_id,
                instrument_id,
                accession,
            },
            Self::Fundamental {
                source_id,
                instrument_id,
                source_record,
                concept,
                unit,
                effective,
            } => ObservationFamilyKey::Fundamental {
                source_id,
                instrument_id,
                source_record,
                concept,
                unit,
                effective: effective.into_domain()?,
            },
            Self::Macro {
                source_id,
                series,
                effective,
            } => ObservationFamilyKey::Macro {
                source_id,
                series,
                effective: effective.into_domain()?,
            },
            Self::PortfolioPosition {
                source_id,
                instrument_id,
                account_id,
                effective,
            } => ObservationFamilyKey::PortfolioPosition {
                source_id,
                instrument_id,
                account_id,
                effective: effective.into_domain()?,
            },
            Self::Transaction {
                source_id,
                instrument_id,
                account_id,
                source_record_id,
            } => ObservationFamilyKey::Transaction {
                source_id,
                instrument_id,
                account_id,
                source_record_id,
            },
            Self::CorporateAction {
                source_id,
                instrument_id,
                source_record,
            } => ObservationFamilyKey::CorporateAction {
                source_id,
                instrument_id,
                source_record,
            },
            Self::UniverseMembership {
                source_id,
                instrument_id,
                source_record,
                universe,
            } => ObservationFamilyKey::UniverseMembership {
                source_id,
                instrument_id,
                source_record,
                universe,
            },
            Self::AlternativeData {
                source_id,
                instrument_id,
                source_record,
                dataset,
                field,
                effective,
            } => ObservationFamilyKey::AlternativeData {
                source_id,
                instrument_id,
                source_record,
                dataset,
                field,
                effective: effective.into_domain()?,
            },
        })
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "precision",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum TemporalCoordinateDto {
    ExactTimestamp {
        unix_nanos: i64,
    },
    CalendarDate {
        year: u16,
        month: u8,
        day: u8,
    },
    SourcePeriod {
        scheme: SourceIdentifier,
        year: u16,
        ordinal: u16,
        code: SourceIdentifier,
    },
}

impl TemporalCoordinateDto {
    fn into_domain(self) -> Result<ResearchTemporalCoordinate, CliDatasetError> {
        match self {
            Self::ExactTimestamp { unix_nanos } => {
                Ok(ResearchTemporalCoordinate::exact(timestamp(unix_nanos)))
            }
            Self::CalendarDate { year, month, day } => {
                Ok(ResearchTemporalCoordinate::calendar_date(
                    CalendarDate::new(year, month, day)
                        .map_err(|_| CliDatasetError::InvalidRequest)?,
                ))
            }
            Self::SourcePeriod {
                scheme,
                year,
                ordinal,
                code,
            } => Ok(ResearchTemporalCoordinate::source_period(
                ResearchPeriod::try_new(
                    scheme,
                    year,
                    NonZeroU16::new(ordinal).ok_or(CliDatasetError::InvalidRequest)?,
                    code,
                )
                .map_err(|_| CliDatasetError::InvalidRequest)?,
            )),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PolicyDto {
    split: SplitPolicyDto,
    point_in_time: PointInTimePolicyDto,
    corporate_actions: CorporateActionPolicyDto,
    missing_values: MissingValuePolicyDto,
    implementation_revision: SourceIdentifier,
}

impl PolicyDto {
    fn into_domain(self) -> Result<DatasetBuildPolicy, CliDatasetError> {
        Ok(DatasetBuildPolicy::new(
            self.split.into_domain()?,
            self.point_in_time.into_domain()?,
            self.corporate_actions.into_domain()?,
            self.missing_values.into_domain(),
            self.implementation_revision,
        ))
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SplitPolicyDto {
    train_end_unix_nanos: i64,
    validation_end_unix_nanos: i64,
    test_end_unix_nanos: i64,
}

impl SplitPolicyDto {
    fn into_domain(self) -> Result<ChronologicalSplitPolicy, CliDatasetError> {
        ChronologicalSplitPolicy::try_new(
            timestamp(self.train_end_unix_nanos),
            timestamp(self.validation_end_unix_nanos),
            timestamp(self.test_end_unix_nanos),
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PointInTimePolicyDto {
    version: u32,
    revision_mode: RevisionModeDto,
}

impl PointInTimePolicyDto {
    fn into_domain(self) -> Result<PointInTimePolicy, CliDatasetError> {
        PointInTimePolicy::try_new(
            NonZeroU32::new(self.version).ok_or(CliDatasetError::InvalidRequest)?,
            self.revision_mode.into_domain(),
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum RevisionModeDto {
    LatestKnown,
    AllKnown,
}

impl RevisionModeDto {
    const fn into_domain(self) -> PointInTimeRevisionMode {
        match self {
            Self::LatestKnown => PointInTimeRevisionMode::LatestKnown,
            Self::AllKnown => PointInTimeRevisionMode::AllKnown,
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CorporateActionPolicyDto {
    adjustment: CorporateActionAdjustmentDto,
    version: u32,
}

impl CorporateActionPolicyDto {
    fn into_domain(self) -> Result<CorporateActionPolicy, CliDatasetError> {
        Ok(CorporateActionPolicy::new(
            self.adjustment.into_domain(),
            NonZeroU32::new(self.version).ok_or(CliDatasetError::InvalidRequest)?,
        ))
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum CorporateActionAdjustmentDto {
    Raw,
    SplitAdjusted,
    TotalReturn,
}

impl CorporateActionAdjustmentDto {
    const fn into_domain(self) -> CorporateActionAdjustment {
        match self {
            Self::Raw => CorporateActionAdjustment::Raw,
            Self::SplitAdjusted => CorporateActionAdjustment::SplitAdjusted,
            Self::TotalReturn => CorporateActionAdjustment::TotalReturn,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum MissingValuePolicyDto {
    Reject,
    Preserve,
    DropExample,
}

impl MissingValuePolicyDto {
    const fn into_domain(self) -> MissingValuePolicy {
        match self {
            Self::Reject => MissingValuePolicy::Reject,
            Self::Preserve => MissingValuePolicy::Preserve,
            Self::DropExample => MissingValuePolicy::DropExample,
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResearchUseDto {
    Display,
    LocalAnalysis,
    Train,
}

impl ResearchUseDto {
    const fn into_domain(self) -> ResearchUse {
        match self {
            Self::Display => ResearchUse::Display,
            Self::LocalAnalysis => ResearchUse::LocalAnalysis,
            Self::Train => ResearchUse::Train,
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ResearchUseLimitsDto {
    max_roots: usize,
    max_nodes: usize,
    max_edges: usize,
    max_sources: usize,
    max_retained_bytes: usize,
    traversal_deadline_millis: u64,
    permit_lifetime_millis: u64,
}

impl ResearchUseLimitsDto {
    fn into_domain(self) -> Result<ResearchUseLimits, CliDatasetError> {
        ResearchUseLimits::try_new(
            self.max_roots,
            self.max_nodes,
            self.max_edges,
            self.max_sources,
            self.max_retained_bytes,
            Duration::from_millis(self.traversal_deadline_millis),
            Duration::from_millis(self.permit_lifetime_millis),
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct OutputAuthorizationDto {
    source_id: SourceId,
    basis: RightsBasisDto,
    authorization_sha256: String,
    authorization_expires_at_unix_nanos: Option<i64>,
}

impl OutputAuthorizationDto {
    fn into_domain(
        self,
        ownership: UserOwnedInputEvidence,
    ) -> Result<DatasetOutputAuthorization, CliDatasetError> {
        let basis = match self.basis {
            RightsBasisDto::ReviewedTerms { url, terms_sha256 } => {
                RightsBasis::reviewed_terms(url, evidence(&terms_sha256)?)
                    .map_err(|_| CliDatasetError::InvalidRequest)?
            }
            RightsBasisDto::RequestFileOwnership => RightsBasis::user_owned_local(ownership),
        };
        DatasetOutputAuthorization::try_new(
            self.source_id,
            basis,
            evidence(&self.authorization_sha256)?,
            self.authorization_expires_at_unix_nanos.map(timestamp),
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "snake_case",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
enum RightsBasisDto {
    ReviewedTerms { url: String, terms_sha256: String },
    RequestFileOwnership,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct BuildLimitsDto {
    max_input_rows: usize,
    max_examples: usize,
    max_components_per_example: usize,
    max_output_rows: usize,
    max_retained_bytes: usize,
    max_duration_millis: u64,
    point_in_time: PointInTimeLimitsDto,
    universe: UniverseLimitsDto,
    corporate_actions: CorporateActionLimitsDto,
}

impl BuildLimitsDto {
    fn into_domain(self) -> Result<DatasetBuildLimits, CliDatasetError> {
        DatasetBuildLimits::try_new(
            self.max_input_rows,
            self.max_examples,
            self.max_components_per_example,
            self.max_output_rows,
            self.max_retained_bytes,
            Duration::from_millis(self.max_duration_millis),
            self.point_in_time.into_domain()?,
            self.universe.into_domain()?,
            self.corporate_actions.into_domain()?,
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PointInTimeLimitsDto {
    max_candidates: usize,
    max_families: usize,
    max_conflicts: usize,
    max_result_rows: usize,
    max_retained_bytes: usize,
}

impl PointInTimeLimitsDto {
    fn into_domain(self) -> Result<PointInTimeLimits, CliDatasetError> {
        PointInTimeLimits::try_new(
            self.max_candidates,
            self.max_families,
            self.max_conflicts,
            self.max_result_rows,
            self.max_retained_bytes,
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct UniverseLimitsDto {
    max_candidates: usize,
    max_retained_bytes: usize,
}

impl UniverseLimitsDto {
    fn into_domain(self) -> Result<UniverseLimits, CliDatasetError> {
        UniverseLimits::try_new(self.max_candidates, self.max_retained_bytes)
            .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CorporateActionLimitsDto {
    max_actions: usize,
    max_retained_bytes: usize,
}

impl CorporateActionLimitsDto {
    fn into_domain(self) -> Result<CorporateActionLimits, CliDatasetError> {
        CorporateActionLimits::try_new(
            NonZeroUsize::new(self.max_actions).ok_or(CliDatasetError::InvalidRequest)?,
            NonZeroUsize::new(self.max_retained_bytes).ok_or(CliDatasetError::InvalidRequest)?,
        )
        .map_err(|_| CliDatasetError::InvalidRequest)
    }
}

fn convert_all<T, U>(
    values: Vec<T>,
    convert: impl Fn(T) -> Result<U, CliDatasetError>,
) -> Result<Vec<U>, CliDatasetError> {
    values.into_iter().map(convert).collect()
}

fn evidence(value: &str) -> Result<EvidenceDigest, CliDatasetError> {
    Ok(EvidenceDigest::new(
        DigestAlgorithm::Sha256,
        decode_nonzero_sha256(value)?,
    ))
}

fn decode_nonzero_sha256(value: &str) -> Result<[u8; 32], CliDatasetError> {
    let bytes = decode_sha256(value)?;
    if bytes == [0; 32] {
        Err(CliDatasetError::InvalidRequest)
    } else {
        Ok(bytes)
    }
}

fn decode_sha256(value: &str) -> Result<[u8; 32], CliDatasetError> {
    if value.len() != 64 {
        return Err(CliDatasetError::InvalidRequest);
    }
    let mut output = [0_u8; 32];
    for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
        let high = nibble(pair[0]).ok_or(CliDatasetError::InvalidRequest)?;
        let low = nibble(pair[1]).ok_or(CliDatasetError::InvalidRequest)?;
        output[index] = (high << 4) | low;
    }
    Ok(output)
}

fn nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        _ => None,
    }
}

const fn timestamp(unix_nanos: i64) -> Timestamp {
    Timestamp::from_unix_nanos(unix_nanos)
}
