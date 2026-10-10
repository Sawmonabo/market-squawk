//! Current label-free price inputs through the existing immutable feature-only reader.

use market_squawk_data::{
    AnalyticalReadCapability, DatasetManifestRef, FeatureDatasetInputCoordinate,
    FeatureDatasetInputEpochOutput, FeatureDatasetProductContract, ForecastFeatureValue,
    QueryLimits,
};
use market_squawk_modeling::{ForecastMeasurement, ForecastTargetMeaning, ModelMetadata};
use market_squawk_services::ServiceError;
use rust_decimal::{Decimal, prelude::ToPrimitive as _};
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

pub(crate) async fn reopen_current_price_input(
    analytical: &AnalyticalReadCapability,
    manifest: &DatasetManifestRef,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<FeatureDatasetInputEpochOutput, ServiceError> {
    use market_squawk_data::{
        AnalyticalReadError as Read, CatalogError as Catalog, ManifestCatalogError as Manifest,
        ParquetStoreError as Parquet, PythonDatasetCatalogError as Python, QueryError as Query,
        ResearchUseCatalogError as ResearchUse,
    };
    use market_squawk_platform::ResearchObjectControlError as Control;

    fn catalog(error: Catalog) -> ServiceError {
        match error {
            Catalog::QueryArtifactCancelled
            | Catalog::InstrumentDefinitionReadCancelled
            | Catalog::CompanyIdentityReadCancelled
            | Catalog::MarketRecoveryReadCancelled
            | Catalog::AnalyticalEvidenceCancelled => ServiceError::Cancelled,
            Catalog::OnboardingDeadlineExceeded
            | Catalog::QueryArtifactDeadlineExceeded
            | Catalog::InstrumentDefinitionReadDeadlineExceeded
            | Catalog::CompanyIdentityReadDeadlineExceeded
            | Catalog::MarketRecoveryReadDeadlineExceeded => ServiceError::DeadlineExceeded,
            Catalog::InvalidLimit
            | Catalog::ResultByteLimitExceeded
            | Catalog::ResultRowLimitExceeded
            | Catalog::AnalyticalEvidenceLimitExceeded
            | Catalog::Allocation => ServiceError::ResourceExhausted,
            Catalog::RightsDenied(_)
            | Catalog::RightsNotAdmitted
            | Catalog::InvalidRightsCapability => ServiceError::Unauthorized,
            _ => ServiceError::Internal,
        }
    }

    fn research_use(error: ResearchUse) -> ServiceError {
        match error {
            ResearchUse::Cancelled => ServiceError::Cancelled,
            ResearchUse::DeadlineExceeded => ServiceError::DeadlineExceeded,
            ResearchUse::LimitExceeded => ServiceError::ResourceExhausted,
            ResearchUse::Denied { .. }
            | ResearchUse::Expired
            | ResearchUse::Revoked
            | ResearchUse::InvalidPermitSession => ServiceError::Unauthorized,
            ResearchUse::Catalog(error) => catalog(error),
            _ => ServiceError::Internal,
        }
    }

    fn parquet(error: Parquet) -> ServiceError {
        match error {
            Parquet::Cancelled => ServiceError::Cancelled,
            Parquet::ReadDeadlineExceeded | Parquet::RecoveryDeadlineExceeded => {
                ServiceError::DeadlineExceeded
            }
            Parquet::StagingLimitExceeded
            | Parquet::ReadLimitExceeded
            | Parquet::SizeOverflow
            | Parquet::BlockingTaskLimitExceeded
            | Parquet::RecoveryScanLimit => ServiceError::ResourceExhausted,
            Parquet::ContentAddressConflict
            | Parquet::ObjectMetadataMismatch
            | Parquet::RootCatalogMismatch => ServiceError::InvalidResult,
            _ => ServiceError::Internal,
        }
    }

    fn classify(error: Read) -> ServiceError {
        match error {
            // Only an absent exact catalog selection is routine source unavailability.
            Read::ForecastDatasetUnavailable => ServiceError::Unavailable,
            Read::NativeSessionControl(Control::Cancelled) => ServiceError::Cancelled,
            Read::NativeSessionControl(Control::DeadlineExceeded) => ServiceError::DeadlineExceeded,
            Read::InvalidLimit | Read::InputEpochResultRequiresInline => {
                ServiceError::ResourceExhausted
            }
            Read::InvalidInputEpoch => ServiceError::InvalidResult,
            Read::Manifest(error) => match error {
                Manifest::Cancelled => ServiceError::Cancelled,
                Manifest::DeadlineExceeded => ServiceError::DeadlineExceeded,
                Manifest::ObjectLimitExceeded { .. }
                | Manifest::CaptureInputLimitExceeded { .. }
                | Manifest::MarketBarHistoryInputLimitExceeded { .. }
                | Manifest::FundNavInputLimitExceeded { .. }
                | Manifest::ReferenceWorkLimitExceeded { .. }
                | Manifest::FeatureDatasetCandidateLimitExceeded { .. }
                | Manifest::CountOverflow => ServiceError::ResourceExhausted,
                Manifest::PopulationResearchUse(error) => research_use(*error),
                Manifest::CatalogAuthority(error) => catalog(error),
                Manifest::AnchorMismatch
                | Manifest::SchemaMismatch
                | Manifest::SchemaIdentity(_)
                | Manifest::CorruptCatalog
                | Manifest::MarketBarHistoryMismatch
                | Manifest::FundNavPublicationMismatch
                | Manifest::ProviderMacroPlanMismatch => ServiceError::InvalidResult,
                _ => ServiceError::Internal,
            },
            Read::PythonDataset(error) => match error {
                Python::Cancelled => ServiceError::Cancelled,
                Python::DeadlineExceeded => ServiceError::DeadlineExceeded,
                Python::LimitExceeded => ServiceError::ResourceExhausted,
                Python::PopulationResearchUse(error) => research_use(*error),
                Python::Catalog(error) => catalog(error),
                Python::ResearchAuthorizationExpired => ServiceError::Unauthorized,
                Python::CorruptAdmission
                | Python::InvalidProductionEvidence
                | Python::ConflictingProductionAdmission => ServiceError::InvalidResult,
                _ => ServiceError::Internal,
            },
            Read::Parquet(error) => parquet(error),
            Read::Query(error) => match error {
                Query::Cancelled => ServiceError::Cancelled,
                Query::DeadlineExceeded => ServiceError::DeadlineExceeded,
                Query::InvalidLimits
                | Query::AstLimitExceeded
                | Query::PlanLimitExceeded
                | Query::PartitionLimitExceeded
                | Query::RowLimitExceeded { .. }
                | Query::ByteLimitExceeded { .. }
                | Query::MemoryLimitExceeded { .. }
                | Query::SizeOverflow
                | Query::BlockingTaskLimitExceeded
                | Query::ReaderMemoryBoundExceeded => ServiceError::ResourceExhausted,
                Query::Artifact(error) => parquet(error),
                Query::Catalog(error) => catalog(error),
                Query::InvalidSource | Query::ManifestPinMismatch | Query::ArrowConversion(_) => {
                    ServiceError::InvalidResult
                }
                _ => ServiceError::Internal,
            },
            _ => ServiceError::Internal,
        }
    }

    let limits = QueryLimits::try_new_with_inline_bytes(
        4096,
        32 * 1024 * 1024,
        64 * 1024 * 1024,
        64 * 1024 * 1024,
        128,
        128,
        128,
        Duration::from_secs(30),
    )
    .map_err(|_| ServiceError::ResourceExhausted)?;
    analytical
        .feature_dataset_input_epochs(
            FeatureDatasetProductContract::PriceReturnMacroContextFixedHorizonStudyInputsV1,
            manifest,
            limits,
            deadline,
            cancellation,
        )
        .await
        .map_err(classify)
}

pub(crate) fn current_price_coordinate_index(
    output: &FeatureDatasetInputEpochOutput,
    example_id: &str,
) -> Result<usize, ServiceError> {
    let mut found = output
        .epochs()
        .iter()
        .enumerate()
        .filter(|(_, epoch)| epoch.example_id() == example_id);
    let (index, _) = found.next().ok_or(ServiceError::NotFound)?;
    if found.next().is_some() {
        return Err(ServiceError::InvalidResult);
    }
    Ok(index)
}

/// Uses model metadata order and exact original component versions. No later label is read.
pub(crate) fn current_price_feature_values(
    metadata: &ModelMetadata,
    coordinate: FeatureDatasetInputCoordinate<'_>,
) -> Result<Vec<f64>, ServiceError> {
    let epoch = coordinate.epoch();
    market_squawk_modeling::ForecastCurrentPriceServingRecord::from_coordinate(coordinate)
        .map_err(|_| ServiceError::InvalidRequest)?;
    let binding = metadata.output_binding();
    let compatible = matches!(binding.target(), ForecastTargetMeaning::FixedHorizonTerminal { horizon_nanos, origin_basis } | ForecastTargetMeaning::FixedHorizonEvent { horizon_nanos, origin_basis, .. }
        if Some(origin_basis) == epoch.fixed_horizon_origin_basis()
        && epoch.target_at().zip(epoch.target_origin()).is_some_and(|(target, origin)|
            target.unix_nanos().checked_sub(origin.unix_nanos()) == i64::try_from(horizon_nanos.get()).ok()));
    if !matches!(
        (binding.measurement(), binding.target()),
        (
            ForecastMeasurement::Return,
            ForecastTargetMeaning::FixedHorizonTerminal { .. }
        ) | (
            ForecastMeasurement::Probability,
            ForecastTargetMeaning::FixedHorizonEvent { .. }
        )
    ) || !compatible
        || !market_squawk_modeling::has_price_return_macro_context_feature_order_v1(metadata)
        || coordinate.rows().len() != metadata.features().len()
    {
        return Err(ServiceError::InvalidRequest);
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(metadata.features().len())
        .map_err(|_| ServiceError::ResourceExhausted)?;
    for binding in metadata.features() {
        let mut rows = coordinate.rows().iter().filter(|row| {
            row.example_id() == epoch.example_id()
                && row.instrument_id() == epoch.instrument_id()
                && row.source_selection_as_of() == epoch.source_selection_as_of()
                && row.decision_coordinate() == epoch.decision_coordinate()
                && row.label_selection_as_of().is_none()
                && row.label_effective_at() == epoch.target_at()
                && row.observed_effective_at() == epoch.target_origin()
                && row.component_kind() == 1
                && row.component_name() == binding.key().name()
                && row.component_version() == binding.key().version().get()
        });
        let row = rows.next().ok_or(ServiceError::Unavailable)?;
        if rows.next().is_some() {
            return Err(ServiceError::InvalidResult);
        }
        let value = match row.value() {
            ForecastFeatureValue::Float(value) => *value,
            ForecastFeatureValue::Decimal { mantissa, scale } => {
                Decimal::try_from_i128_with_scale(*mantissa, u32::from(*scale))
                    .ok()
                    .and_then(|value| value.to_f64())
                    .ok_or(ServiceError::InvalidResult)?
            }
            ForecastFeatureValue::Missing => return Err(ServiceError::Unavailable),
        };
        if !value.is_finite() {
            return Err(ServiceError::InvalidResult);
        }
        values.push(value);
    }
    Ok(values)
}

/// Reopens the original application calendar and binds the genuinely reread source epoch.
pub(crate) async fn current_price_session_origin(
    calendar: Option<&crate::application::market_calendar::ForecastSessionReadCapability>,
    reference: Option<&crate::application::market_calendar::ForecastSessionCohortReference>,
    coordinate: FeatureDatasetInputCoordinate<'_>,
    deadline: Instant,
    cancellation: CancellationToken,
) -> Result<Option<crate::application::market_calendar::ForecastSessionOrigin>, ServiceError> {
    let Some(reference) = reference else {
        return Ok(None);
    };
    let calendar = calendar.ok_or(ServiceError::Unavailable)?;
    let cohort = calendar
        .read_cohort(reference, deadline, cancellation.clone())
        .await
        .map_err(map_calendar_error)?
        .ok_or(ServiceError::Unavailable)?;
    cohort
        .bind_input_epoch(coordinate)
        .map_err(|_| ServiceError::InvalidResult)?
        .map(Some)
        .ok_or(ServiceError::Unavailable)
}

pub(crate) fn current_price_cohort_reference(
    input: &market_squawk_modeling::ForecastCurrentPriceServingRecord,
) -> Result<Option<crate::application::market_calendar::ForecastSessionCohortReference>, ServiceError>
{
    input
        .session_cohort_json
        .as_ref()
        .map(|value| {
            let reference: crate::application::market_calendar::ForecastSessionCohortReference =
                serde_json::from_str(value).map_err(|_| ServiceError::InvalidRequest)?;
            reference
                .validate()
                .map_err(|_| ServiceError::InvalidRequest)?;
            if serde_json::to_string(&reference).map_err(|_| ServiceError::InvalidResult)? != *value
            {
                return Err(ServiceError::InvalidRequest);
            }
            Ok(reference)
        })
        .transpose()
}

/// One current source return, displayed under the existing eight-place forecast history policy.
/// The trained estimator receives the untouched current feature value separately.
pub(crate) fn current_price_observed_point(
    coordinate: FeatureDatasetInputCoordinate<'_>,
    value: f64,
) -> Result<market_squawk_modeling::ForecastObservedPoint, ServiceError> {
    let epoch = coordinate.epoch();
    let mut display = Decimal::from_f64_retain(value)
        .ok_or(ServiceError::InvalidResult)?
        .round_dp(8);
    display.rescale(8);
    let value = market_squawk_modeling::ForecastValue::try_new(display.mantissa(), 8)
        .map_err(|_| ServiceError::InvalidResult)?;
    let digest =
        market_squawk_modeling::ForecastCurrentPriceServingRecord::feature_identity(coordinate)
            .map_err(|_| ServiceError::InvalidResult)?;
    market_squawk_modeling::ForecastObservedPoint::try_new(
        epoch.target_origin().ok_or(ServiceError::InvalidRequest)?,
        epoch.source_selection_as_of(),
        value,
        digest,
        market_squawk_domain::DataQuality::Aggregated,
    )
    .map_err(|_| ServiceError::InvalidResult)
}

fn map_calendar_error(
    error: crate::application::market_calendar::CompletedMarketSessionError,
) -> ServiceError {
    use crate::application::market_calendar::CompletedMarketSessionError as Error;
    match error {
        Error::Cancelled => ServiceError::Cancelled,
        Error::DeadlineExceeded => ServiceError::DeadlineExceeded,
        Error::ResourceBoundExceeded => ServiceError::ResourceExhausted,
        Error::InvalidRequest => ServiceError::InvalidRequest,
        Error::InvalidEvidence => ServiceError::InvalidResult,
        Error::Unavailable => ServiceError::Unavailable,
    }
}
