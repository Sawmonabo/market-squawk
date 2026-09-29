//! Provider-neutral immutable final Find operations; orchestration stays with the native workflow.
use super::current_find::{InstalledCurrentFind, PreparationReference};
use crate::application::decision::{
    current_find::CurrentFindPreparationRecord,
    find_results::{FindAnalysisReference, FindResultsRecord, FindScreenJobReference},
};
use market_squawk_services::{
    RequestContext, ServiceError, ToolResultMetadata, TypedToolRequest, TypedToolResult,
};
use serde::Deserialize;
use serde_json::{Value, json};
use uuid::Uuid;

pub(super) const PUBLISH: &str = "Decision.PublishFindResults";
pub(super) const GET: &str = "Decision.GetFindResults";

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PublishInput {
    preparation_id: Uuid,
    preparation_sha256: String,
    screen_job: Option<FindScreenJobReference>,
    analyses: Vec<FindAnalysisReference>,
}
impl InstalledCurrentFind {
    pub(super) async fn call_find_results(
        &self,
        request: &TypedToolRequest,
        context: &RequestContext,
        training: &super::training_preparation::InstalledProductTraining,
    ) -> Result<TypedToolResult, ServiceError> {
        self.authorize(context)?;
        let (parent, record) = match request.name() {
            PUBLISH => {
                let input: PublishInput = super::decode(request.arguments())?;
                let parent = self.parent(
                    &PreparationReference {
                        preparation_id: input.preparation_id,
                        preparation_sha256: input.preparation_sha256,
                    },
                    context,
                )?;
                if input.analyses.len() > parent.population.maximum_deep_analyses() {
                    return Err(ServiceError::InvalidRequest);
                }
                // Retry returns the exact saved publication before any mutable job/source reads.
                if let Some(existing) = self.decisions.find_results(&parent).map_err(map)? {
                    if existing.screen_job != input.screen_job
                        || !existing
                            .rows
                            .iter()
                            .map(|r| &r.reference)
                            .eq(input.analyses.iter())
                    {
                        return Err(ServiceError::InvalidRequest);
                    }
                    (parent, existing)
                } else {
                    match input.screen_job.as_ref() {
                        Some(job) => {
                            self.validate_screen_job(
                                &parent,
                                job.job_id,
                                job.generation,
                                training,
                                context,
                            )
                            .await?;
                        }
                        None => {
                            let (coverage, available) = self.coverage_value(&parent)?;
                            if coverage.get("complete") != Some(&Value::Bool(true))
                                || available != 0
                                || !input.analyses.is_empty()
                            {
                                return Err(ServiceError::InvalidRequest);
                            }
                        }
                    }
                    let record = self.decisions.publish_find_results(
                        &parent,
                        input.screen_job,
                        input.analyses,
                        context,
                    )?;
                    (parent, record)
                }
            }
            GET => {
                let input: PreparationReference = super::decode(request.arguments())?;
                let parent = self.parent(&input, context)?;
                let record = self
                    .decisions
                    .find_results(&parent)
                    .map_err(map)?
                    .ok_or(ServiceError::NotFound)?;
                (parent, record)
            }
            _ => return Err(ServiceError::NotFound),
        };
        let value = self.find_results_value(&parent, &record)?;
        self.authorize(context)?;
        TypedToolResult::try_new(
            value,
            record.rows.len(),
            ToolResultMetadata::complete_not_applicable(),
            context.limits(),
        )
        .map_err(Into::into)
    }
    fn find_results_value(
        &self,
        parent: &CurrentFindPreparationRecord,
        record: &FindResultsRecord,
    ) -> Result<Value, ServiceError> {
        let estimates = self.decisions.find_result_estimates(record).map_err(map)?;
        let results=record.ranked_indices.iter().enumerate().map(|(rank,&index)|{
            let row=&record.rows[index];
            let (state,analysis_id,unavailable)=if let Some(member)=&row.member_unavailable {
                ("unavailable",None,Some(member.clone()))
            } else {
                let bytes=row.analysis_id.ok_or(ServiceError::InvalidResult)?;
                let id=market_squawk_decisions::InvestmentAnalysisId::try_from_bytes(bytes).map_err(|_|ServiceError::InvalidResult)?;
                let bundle=self.decisions.get_prepared_published_investment_analysis(id).map_err(map)?;
                let state=match bundle.decision(){
                    market_squawk_decisions::InvestmentProposalDecision::Generated(_)=>"generated",
                    market_squawk_decisions::InvestmentProposalDecision::NoAction(_)=>"no_action",
                    market_squawk_decisions::InvestmentProposalDecision::Unavailable(_)=>"unavailable",
                };
                (state,Some(hex(bytes)),None)
            };
            let mut value=json!({"analysisState":state,"candidateId":row.reference.candidate_id,"actionToken":row.reference.action_token,"analysisId":analysis_id,"screenRank":index+1,"rank":rank+1,"expectedReturn":super::super::decision::find_expected_return_value(estimates[index])});
            if let Some(unavailable)=unavailable {value["memberUnavailable"]=serde_json::to_value(unavailable).map_err(|_|ServiceError::InvalidResult)?;}
            Ok(value)
        }).collect::<Result<Vec<_>,ServiceError>>()?;
        let reference = json!({"preparationId":record.preparation_id,"preparationSha256":hex(record.preparation_sha256)});
        Ok(
            json!({"preparationId":record.preparation_id,"preparationSha256":hex(record.preparation_sha256),"resultSha256":hex(record.digest().map_err(map)?),"ordering":if record.comparable_horizons {"estimated_gain_descending"} else {"unavailable_incomparable_horizons"},"screenRunId":record.screen_run_id,"coverageReference":reference,"coverage":self.coverage_value(parent)?.0,"results":results,"benchmarks":{"primary":"SPY","alongside":"VTI"}}),
        )
    }
}
fn hex(bytes: [u8; 32]) -> String {
    crate::application::model::forecast_preparation::hex(market_squawk_data::Sha256Digest::new(
        bytes,
    ))
}
fn map(error: crate::application::decision::DecisionApplicationError) -> ServiceError {
    super::super::decision::map_application(error)
}
