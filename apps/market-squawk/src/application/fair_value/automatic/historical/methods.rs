//! Four actual historical method attempts over one original economic/source coordinate.
use super::*;
use crate::application::market_calendar::CompletedMarketSessionReadCapability;
use crate::application::research::{HistoricalOriginFinancialForecast, MacroContextReadCapability};
use market_squawk_data::AuthorizedResearchUse;
use market_squawk_valuation::AutomaticValuationMethod;

/// Native total equity is never relabeled per share. Reported EPS/raw-price comparables require
/// additional common share-unit evidence before their arithmetic can govern an entry decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum HistoricalValueBasis {
    TotalCommonEquity,
    AsReportedShareUnits,
}

#[derive(Clone, Debug)]
pub(super) struct HistoricalMethodReceipt {
    method: AutomaticValuationMethod,
    basis: HistoricalValueBasis,
    epoch_identity: Sha256Digest,
    identity: EvidenceDigest,
    source_identity: EvidenceDigest,
    rights_decision: ResearchUseDecisionDigest,
    rights_graph: ResearchUseGraphDigest,
    roots: Box<[DatasetManifestRef]>,
    value: Money,
    lower: Money,
    upper: Money,
    calculated_at: Timestamp,
    expires_at: Timestamp,
}

/// Existing source readers and one compact controlled source recipe. Only the current original
/// origin's at-most-nine forecasts are materialized; no whole-study native forecast arrays remain.
pub(crate) struct HistoricalStudyValuationReadCapability {
    macro_reader: MacroContextReadCapability,
    calendars: CompletedMarketSessionReadCapability,
    reader: std::sync::Arc<crate::application::research::HistoricalFiscalForecastReadCapability>,
    source: crate::application::research::HistoricalFiscalSourceSelection,
    profile: crate::application::analytical_profile::ValidatedAnalyticalProfile,
}
impl HistoricalStudyValuationReadCapability {
    pub(crate) fn new(
        macro_reader: MacroContextReadCapability,
        calendars: CompletedMarketSessionReadCapability,
        reader: std::sync::Arc<
            crate::application::research::HistoricalFiscalForecastReadCapability,
        >,
        source: crate::application::research::HistoricalFiscalSourceSelection,
        profile: crate::application::analytical_profile::ValidatedAnalyticalProfile,
    ) -> Self {
        Self {
            macro_reader,
            calendars,
            reader,
            source,
            profile,
        }
    }
    pub(crate) fn reference(
        &self,
    ) -> &crate::application::research::HistoricalFiscalRecipeReference {
        self.source.reference()
    }
    pub(crate) fn fiscal_identity(&self) -> Result<Sha256Digest, ServiceError> {
        self.source.reference().identity()
    }
    pub(crate) async fn reopen(
        macro_reader: MacroContextReadCapability,
        calendars: CompletedMarketSessionReadCapability,
        reader: std::sync::Arc<
            crate::application::research::HistoricalFiscalForecastReadCapability,
        >,
        reference: &crate::application::research::HistoricalFiscalRecipeReference,
        profile: &crate::application::analytical_profile::ValidatedAnalyticalProfile,
        context: &RequestContext,
    ) -> Result<Self, ServiceError> {
        let source = reader.read_selection(reference, profile, context).await?;
        Ok(Self::new(
            macro_reader,
            calendars,
            reader,
            source,
            profile.clone(),
        ))
    }
    async fn origin_forecasts(
        &self,
        epoch: &FeatureDatasetInputEpoch,
        context: &RequestContext,
    ) -> Result<Vec<HistoricalOriginFinancialForecast>, ServiceError> {
        self.reader
            .read_origin(&self.source, epoch, &self.profile, context)
            .await
    }
}

/// Completed methods are kept independently. Only the source-qualified per-unit price forecast
/// currently supplies the entry-zone value; total equity or unproven share units cannot replace it.
pub(crate) struct HistoricalValuationMethodEvaluation {
    predictive: Result<HistoricalForecastValuationReceipt, ServiceError>,
    audit: Value,
}
impl HistoricalValuationMethodEvaluation {
    pub(crate) fn audit(&self) -> &Value {
        &self.audit
    }
    pub(crate) fn predictive(&self) -> Result<&HistoricalForecastValuationReceipt, ServiceError> {
        self.predictive.as_ref().map_err(|e| *e)
    }
}

impl FairValueDomainService {
    pub(crate) async fn evaluate_historical_investment_valuations(
        &self,
        research: &ResearchService,
        forecast: &HistoricalPriceForecast,
        sources: &HistoricalStudyValuationReadCapability,
        request: AutomaticForecastValuationRequest,
        context: &RequestContext,
    ) -> Result<HistoricalValuationMethodEvaluation, ServiceError> {
        ensure_request_live(context, &self.lifecycle)?;
        let epoch = forecast.epoch();
        let origin = epoch.target_origin().ok_or(ServiceError::InvalidRequest)?;
        let date = origin
            .utc_calendar_date()
            .map_err(|_| ServiceError::InvalidRequest)?;
        let macro_context = sources
            .macro_reader
            .read_investment_context(
                epoch.source_selection_as_of(),
                date,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await;
        check_control(&macro_context)?;
        let premium = sources
            .macro_reader
            .read_historical_origin_equity_premium(
                research,
                &sources.calendars,
                epoch,
                context.deadline(),
                context.cancellation().child_token(),
            )
            .await
            .map_err(|error| error.into_service_error());
        check_control(&premium)?;
        let fiscal = sources.origin_forecasts(epoch, context).await?;
        let mut audits = Vec::with_capacity(4);
        for method in [
            AutomaticValuationMethod::DiscountedCashFlow,
            AutomaticValuationMethod::ComparableCompanies,
            AutomaticValuationMethod::ResidualIncome,
        ] {
            ensure_request_live(context, &self.lifecycle)?;
            let started_at = calculation_clock()?;
            let result = if method == AutomaticValuationMethod::ComparableCompanies {
                self.calculate_historical_comparable_receipt(research, &sources.calendars, epoch, &request, context)
                    .await
            } else {
                match (&macro_context, &premium) {
                    (Ok(rates), Ok(premium)) => {
                        self.calculate_historical_native_valuation(
                            research, epoch, &fiscal, rates, premium, method, &request, context,
                        )
                        .await
                    }
                    (Err(error), _) | (_, Err(error)) => Err(*error),
                }
            };
            check_control(&result)?;
            audits.push(method_audit(
                method,
                started_at,
                calculation_clock()?,
                &result,
            ));
        }
        ensure_request_live(context, &self.lifecycle)?;
        let started_at = calculation_clock()?;
        let predictive = self
            .calculate_historical_forecast_valuation(research, forecast, request, context)
            .await;
        check_control(&predictive)?;
        audits.push(match &predictive {
            Ok(receipt) => serde_json::json!({"method":"predictive_terminal_price_expectation","startedAt":started_at,"completedAt":calculation_clock()?,"result":{"identity":receipt.identity().bytes(),"sourceIdentity":receipt.distribution_identity().bytes(),"value":receipt.value(),"lower":receipt.lower(),"upper":receipt.upper(),"basis":"per_instrument_unit","rightsDecision":receipt.rights_decision().bytes(),"rightsGraph":receipt.rights_graph().bytes(),"calculatedAt":receipt.calculated_at(),"expiresAt":receipt.expires_at()}}),
            Err(error) => serde_json::json!({"method":"predictive_terminal_price_expectation","startedAt":started_at,"completedAt":calculation_clock()?,"failure":format!("{error:?}")}),
        });
        let audit = serde_json::json!({"version":1,"fiscalSelectionIdentity":sources.fiscal_identity()?.bytes(),"economicOrigin":origin,"knowledgeCutoff":epoch.source_selection_as_of(),"studyBasis":epoch.basis(),"studyLimitations":epoch.limitations(),"methods":audits});
        Ok(HistoricalValuationMethodEvaluation { predictive, audit })
    }

    async fn calculate_historical_comparable_receipt(
        &self,
        research: &ResearchService,
        calendars: &CompletedMarketSessionReadCapability,
        epoch: &FeatureDatasetInputEpoch,
        request: &AutomaticForecastValuationRequest,
        context: &RequestContext,
    ) -> Result<HistoricalMethodReceipt, ServiceError> {
        let sources = self
            .select_historical_comparable_sources(
                research,
                calendars,
                epoch,
                ObservedComparableValuationRequest {
                    account_id: request.account_id,
                    subject: epoch.instrument_id(),
                    peers: Vec::new(),
                    knowledge_at: epoch.source_selection_as_of(),
                    effective_date: epoch
                        .target_origin()
                        .ok_or(ServiceError::InvalidRequest)?
                        .utc_calendar_date()
                        .map_err(|_| ServiceError::InvalidRequest)?,
                    expires_at: request.expires_at,
                    calculated_by: request.calculated_by.clone(),
                },
                context,
            )
            .await?;
        let mut roots = Vec::new();
        let mut hash = Sha256::new();
        hash.update(b"market-squawk/historical-comparable-as-reported-share-units/v1\0");
        for source in std::iter::once(&sources.subject).chain(sources.peers.iter()) {
            for parent in source
                .fundamentals
                .roots
                .iter()
                .chain(std::iter::once(&source.price.manifest))
                .chain(source.price.native_parents.iter())
            {
                if !roots.contains(parent) {
                    roots.push(parent.clone());
                }
            }
            for digest in [
                source.fundamentals.metric.id().bytes(),
                source.fundamentals.identity.receipt_digest().bytes(),
                source.fundamentals.industry_evidence.bytes(),
                source.price.object_graph.bytes(),
                source.price.query_identity.bytes(),
                source.price.result_identity.bytes(),
            ] {
                hash.update(digest);
            }
            if let Some(native) = source.price.native_evidence {
                hash.update(native.bytes());
            }
            if let Some(lookup) = source.price.history_lookup {
                hash.update(lookup.bytes());
            }
            if source.fundamentals.metric.amount().basis()
                != ValuationAmountBasis::PerInstrumentUnit
            {
                return Err(ServiceError::Unavailable);
            }
        }
        if let Some(cohort) = sources.cohort_evidence {
            hash.update(cohort.bytes());
        }
        let authorization = authorize_sources(research, &roots, context)?;
        let arithmetic = observed_comparable_arithmetic(
            sources
                .subject
                .fundamentals
                .metric
                .amount()
                .money()
                .amount(),
            sources.peers.iter().map(|peer| {
                (
                    peer.price.bar.close().amount(),
                    peer.fundamentals.metric.amount().money().amount(),
                )
            }),
        )?;
        // Actual arithmetic is retained as research only. The selected EPS does not prove that
        // source share restatements use the original raw bar's share units; no entry use is minted.
        issue_method_receipt(
            epoch,
            AutomaticValuationMethod::ComparableCompanies,
            HistoricalValueBasis::AsReportedShareUnits,
            (
                arithmetic.raw_value(),
                arithmetic.lower(),
                arithmetic.upper(),
            ),
            request,
            EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
            roots,
            authorization,
            context,
        )
    }
}

fn check_control<T>(result: &Result<T, ServiceError>) -> Result<(), ServiceError> {
    match result {
        Err(
            error @ (ServiceError::Cancelled
            | ServiceError::DeadlineExceeded
            | ServiceError::ResourceExhausted
            | ServiceError::InvalidResult
            | ServiceError::Internal),
        ) => Err(*error),
        _ => Ok(()),
    }
}

pub(super) fn authorize_sources(
    research: &ResearchService,
    roots: &[DatasetManifestRef],
    context: &RequestContext,
) -> Result<AuthorizedResearchUse, ServiceError> {
    if roots.is_empty() || roots.len() > 64 {
        return Err(ServiceError::ResourceExhausted);
    }
    let duration = context
        .deadline()
        .saturating_duration_since(std::time::Instant::now())
        .min(Duration::from_secs(5));
    if duration.is_zero() {
        return Err(ServiceError::DeadlineExceeded);
    }
    let authorization = research
        .analytical()
        .authorize_research_use(
            ResearchUseRequest::try_new(
                roots.to_vec(),
                ResearchUse::LocalAnalysis,
                ResearchUseLimits::try_new(
                    64,
                    4096,
                    8192,
                    4096,
                    4 * 1024 * 1024,
                    duration,
                    Duration::from_secs(300),
                )
                .map_err(|_| ServiceError::InvalidRequest)?,
            )
            .map_err(|_| ServiceError::InvalidRequest)?,
            context.cancellation(),
        )
        .map_err(map_study_rights_error)?;
    if authorization.research_use() != ResearchUse::LocalAnalysis
        || authorization.graph().roots().len() != roots.len()
        || roots
            .iter()
            .any(|root| !authorization.graph().roots().contains(root))
    {
        return Err(ServiceError::InvalidResult);
    }
    Ok(authorization)
}

pub(super) fn issue_method_receipt(
    epoch: &FeatureDatasetInputEpoch,
    method: AutomaticValuationMethod,
    basis: HistoricalValueBasis,
    amounts: (Decimal, Decimal, Decimal),
    request: &AutomaticForecastValuationRequest,
    source_identity: EvidenceDigest,
    roots: Vec<DatasetManifestRef>,
    authorization: AuthorizedResearchUse,
    context: &RequestContext,
) -> Result<HistoricalMethodReceipt, ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    let now = calculation_clock()?;
    let expires = request.expires_at.min(authorization.expires_at());
    if now >= expires || amounts.1 > amounts.0 || amounts.0 > amounts.2 {
        return Err(ServiceError::Unavailable);
    }
    let currency = epoch
        .current_unit_price()
        .map_err(|_| ServiceError::Unavailable)?
        .currency();
    let (value, lower, upper) = (
        Money::new(amounts.0, currency),
        Money::new(amounts.1, currency),
        Money::new(amounts.2, currency),
    );
    let epoch_identity = Sha256Digest::new(
        Sha256::digest(
            epoch
                .canonical_bytes()
                .map_err(|_| ServiceError::InvalidResult)?,
        )
        .into(),
    );
    let decision = authorization.decision_digest();
    let graph = authorization.graph().digest();
    let mut hash = Sha256::new();
    hash.update(b"market-squawk/historical-method-receipt/v1\0");
    for digest in [
        epoch_identity.bytes(),
        source_identity.bytes(),
        decision.bytes(),
        graph.bytes(),
    ] {
        hash.update(digest);
    }
    hash.update(request.account_id.as_uuid().as_bytes());
    hash_study_bytes(&mut hash, request.calculated_by.as_str().as_bytes());
    hash.update(
        serde_json::to_vec(&(
            method_name(method),
            basis,
            value,
            lower,
            upper,
            now,
            expires,
        ))
        .map_err(|_| ServiceError::InvalidResult)?,
    );
    let receipt = HistoricalMethodReceipt {
        method,
        basis,
        epoch_identity,
        identity: EvidenceDigest::new(DigestAlgorithm::Sha256, hash.finalize().into()),
        source_identity,
        rights_decision: decision,
        rights_graph: graph,
        roots: roots.into_boxed_slice(),
        value,
        lower,
        upper,
        calculated_at: now,
        expires_at: expires,
    };
    let _permit = authorization.into_permit();
    Ok(receipt)
}

fn method_audit(
    method: AutomaticValuationMethod,
    started_at: Timestamp,
    completed_at: Timestamp,
    result: &Result<HistoricalMethodReceipt, ServiceError>,
) -> Value {
    match result {
        Ok(r) => {
            serde_json::json!({"method":method_name(method),"startedAt":started_at,"completedAt":completed_at,"result":{"identity":r.identity.bytes(),"sourceIdentity":r.source_identity.bytes(),"originIdentity":r.epoch_identity.bytes(),"method":method_name(r.method),"basis":r.basis,"entryValueAdmission":match r.basis {HistoricalValueBasis::TotalCommonEquity=>"requires_source_share_count",HistoricalValueBasis::AsReportedShareUnits=>"share_unit_basis_unproven"},"value":r.value,"lower":r.lower,"upper":r.upper,"rightsDecision":r.rights_decision.bytes(),"rightsGraph":r.rights_graph.bytes(),"parents":r.roots.iter().map(|p|p.content_hash().bytes()).collect::<Vec<_>>(),"calculatedAt":r.calculated_at,"expiresAt":r.expires_at}})
        }
        Err(error) => {
            serde_json::json!({"method":method_name(method),"startedAt":started_at,"completedAt":completed_at,"failure":format!("{error:?}")})
        }
    }
}

fn method_name(method: AutomaticValuationMethod) -> &'static str {
    match method {
        AutomaticValuationMethod::DiscountedCashFlow => "discounted_cash_flow",
        AutomaticValuationMethod::ComparableCompanies => "comparable_companies",
        AutomaticValuationMethod::ResidualIncome => "residual_income",
        AutomaticValuationMethod::ForecastDistribution => "predictive_terminal_price_expectation",
    }
}
