//! Final Find publication reuses the original bounded decision journal and immutable analyses.
use std::cmp::Ordering;

use market_squawk_decisions::{
    DecisionAuthority, ExpectedReturnAvailability, InvestmentAnalysisId, ScreenRunId,
};
use market_squawk_services::{RequestContext, ServiceError};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use super::{
    DecisionApplication, DecisionApplicationError,
    current_find::{CurrentFindCustodyRecord, CurrentFindPreparationRecord},
};

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindAnalysisReference {
    pub(crate) candidate_id: String,
    pub(crate) action_token: Option<Uuid>,
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub(crate) unavailable_assessment_sha256: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindScreenJobReference {
    pub(crate) job_id: Uuid,
    pub(crate) generation: u64,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindResultRow {
    pub(crate) reference: FindAnalysisReference,
    pub(crate) analysis_id: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if="Option::is_none")]
    pub(crate) member_unavailable: Option<super::current_find::member::FindMemberUnavailableValue>,
    pub(crate) outcome_digest: Option<[u8; 32]>,
}
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct FindResultsRecord {
    pub(crate) preparation_id: Uuid,
    pub(crate) preparation_sha256: [u8; 32],
    pub(crate) workspace_id: Uuid,
    pub(crate) profile_sha256: [u8; 32],
    pub(crate) screen_job: Option<FindScreenJobReference>,
    pub(crate) screen_run_id: Option<String>,
    /// Original screen order. Immutable optional projection digest freezes availability at publication.
    pub(crate) rows: Box<[FindResultRow]>,
    /// Zero-based row indices, gain descending, original screen rank for equal/unavailable estimates.
    pub(crate) ranked_indices: Box<[usize]>,
    pub(crate) comparable_horizons: bool,
}
impl FindResultsRecord {
    pub(crate) fn key(&self) -> String {
        key(self.preparation_id)
    }
    pub(crate) fn digest(&self) -> Result<[u8; 32], DecisionApplicationError> {
        self.validate()?;
        Ok(Sha256::digest(serde_json::to_vec(self).map_err(|_| invalid())?).into())
    }
    pub(crate) fn validate(&self) -> Result<(), DecisionApplicationError> {
        if self.preparation_id.is_nil()
            || self.workspace_id.is_nil()
            || self.preparation_sha256 == [0; 32]
            || self.profile_sha256 == [0; 32]
            || self.rows.len() > 32
            || self.ranked_indices.len() != self.rows.len()
            || self.screen_run_id.is_none() != self.screen_job.is_none()
            || (self.screen_run_id.is_none() && !self.rows.is_empty())
            || self
                .screen_job
                .as_ref()
                .is_some_and(|j| j.job_id.is_nil() || j.generation == 0)
            || self.rows.iter().any(|r| {
                r.reference.action_token.is_some_and(|id| id.is_nil())
                    || r.reference.action_token.is_some()
                        == r.reference.unavailable_assessment_sha256.is_some()
                    || r.analysis_id.is_some() != r.reference.action_token.is_some()
                    || r.member_unavailable.is_some()
                        != r.reference.unavailable_assessment_sha256.is_some()
                    || r.reference.candidate_id.is_empty()
                    || r.reference.candidate_id.len() > 256
                    || r.analysis_id == Some([0; 32])
                    || r.outcome_digest == Some([0; 32])
            })
        {
            return Err(invalid());
        }
        let mut seen = [false; 32];
        for &index in &self.ranked_indices {
            if index >= self.rows.len() || seen[index] {
                return Err(invalid());
            }
            seen[index] = true;
        }
        Ok(())
    }
    /// Used during initial publication, restart recovery and reads; no new market/source inputs.
    pub(crate) fn validate_authority(
        &self,
        authority: &DecisionAuthority,
    ) -> Result<Vec<ExpectedReturnAvailability>, DecisionApplicationError> {
        self.validate()?;
        let execution = self
            .screen_run_id
            .as_ref()
            .map(|id| {
                let id = ScreenRunId::try_new(id.clone()).map_err(|_| invalid())?;
                authority
                    .repository()
                    .screen_execution(&id)
                    .ok_or_else(invalid)
            })
            .transpose()?;
        if execution.is_some_and(|e| e.candidates().len() != self.rows.len()) {
            return Err(invalid());
        }
        let mut coordinate = None;
        let mut comparable = true;
        let mut estimates = Vec::new();
        estimates
            .try_reserve_exact(self.rows.len())
            .map_err(|_| DecisionApplicationError::Allocation)?;
        for (index, row) in self.rows.iter().enumerate() {
            let candidate = execution
                .ok_or_else(invalid)?
                .candidates()
                .get(index)
                .ok_or_else(invalid)?
                .record();
            if candidate.id().as_str() != row.reference.candidate_id
                || candidate.rank().get() as usize != index + 1
            {
                return Err(invalid());
            }
            if let Some(member) = &row.member_unavailable {
                member.validate()?;
                let value = member;
                if member.member.preparation_id != self.preparation_id
                    || member.member.preparation_sha256
                        != crate::application::model::forecast_preparation::hex(
                            market_squawk_data::Sha256Digest::new(self.preparation_sha256),
                        )
                    || Some(member.member.screen_run_id.as_str()) != self.screen_run_id.as_deref()
                    || member.member.candidate_id != row.reference.candidate_id
                    || Some(&value.assessment_sha256)
                        != row.reference.unavailable_assessment_sha256.as_ref()
                    || row.outcome_digest.is_some()
                {
                    return Err(invalid());
                }
                estimates.push(
                    ExpectedReturnAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied,
                );
                continue;
            }
            let id = InvestmentAnalysisId::try_from_bytes(row.analysis_id.ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
            if Some(super::investment_analysis_product_token(id)) != row.reference.action_token {
                return Err(invalid());
            }
            let bundle = authority.get_prepared_published_investment_analysis(id)?;
            let candidate = execution
                .ok_or_else(invalid)?
                .candidates()
                .get(index)
                .ok_or_else(invalid)?
                .record();
            let selected = bundle.selected_candidate().ok_or_else(invalid)?;
            let provenance = bundle.request_provenance().ok_or_else(invalid)?;
            if candidate.rank().get() as usize != index + 1
                || (bundle.decision().proposal_id().is_some() && row.outcome_digest.is_none())
                || candidate.id().as_str() != row.reference.candidate_id
                || selected.candidate_id() != candidate.id()
                || selected.screen_run_id() != candidate.screen_run_id()
                || bundle.decision().evidence().instrument_id() != candidate.instrument_id()
                || provenance.workspace_id() != *self.workspace_id.as_bytes()
                || bundle
                    .publication()
                    .analytical_profile()
                    .content_digest()
                    .evidence_digest()
                    .bytes()
                    != self.profile_sha256
            {
                return Err(invalid());
            }
            let estimate = match row.outcome_digest {
                None => {
                    ExpectedReturnAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied
                }
                Some(expected) => {
                    let proposal_id = bundle.decision().proposal_id().ok_or_else(invalid)?;
                    let projection = authority.get_investment_outcome_projection(proposal_id)?;
                    if projection.result_digest().bytes() != expected {
                        return Err(invalid());
                    }
                    projection.expected_return()
                }
            };
            if matches!(estimate, ExpectedReturnAvailability::Available(_)) {
                let forecast = bundle
                    .decision()
                    .evidence()
                    .price_forecast()
                    .ok_or_else(invalid)?;
                let current_coordinate = (
                    bundle.decision().policy().horizon_nanos(),
                    forecast.window().observed_at(),
                    forecast.horizon_at(),
                );
                if let Some(expected) = coordinate {
                    if expected != current_coordinate {
                        comparable = false;
                    }
                } else {
                    coordinate = Some(current_coordinate);
                }
            }
            estimates.push(estimate);
        }
        if comparable != self.comparable_horizons
            || ranked(&estimates, comparable)? != self.ranked_indices.as_ref()
        {
            return Err(invalid());
        }
        Ok(estimates)
    }
}
fn ranked(
    estimates: &[ExpectedReturnAvailability],
    comparable: bool,
) -> Result<Vec<usize>, DecisionApplicationError> {
    let mut indices = Vec::new();
    indices
        .try_reserve_exact(estimates.len())
        .map_err(|_| DecisionApplicationError::Allocation)?;
    // At most 32 rows. Fallible stable insertion avoids hiding arithmetic errors inside sort_by.
    for (index, estimate) in estimates.iter().enumerate() {
        let mut position = indices.len();
        while comparable && position > 0 {
            let before = match (*estimate, estimates[indices[position - 1]]) {
                (
                    ExpectedReturnAvailability::Available(left),
                    ExpectedReturnAvailability::Available(right),
                ) => left.checked_cmp(right).map_err(|_| invalid())? == Ordering::Greater,
                (ExpectedReturnAvailability::Available(_), _) => true,
                _ => false,
            };
            if !before {
                break;
            }
            position -= 1;
        }
        indices.insert(position, index);
    }
    Ok(indices)
}
impl DecisionApplication {
    pub(crate) fn find_results(
        &self,
        parent: &CurrentFindPreparationRecord,
    ) -> Result<Option<FindResultsRecord>, DecisionApplicationError> {
        let state = self.reader()?;
        match state
            .journal
            .current_find_record(&key(parent.preparation_id))?
        {
            None => Ok(None),
            Some(CurrentFindCustodyRecord::Results(record)) => {
                if record.preparation_sha256 != parent.digest()? {
                    return Err(invalid());
                }
                record.validate_authority(&state.authority)?;
                Ok(Some(*record))
            }
            _ => Err(invalid()),
        }
    }
    pub(crate) fn find_result_estimates(
        &self,
        record: &FindResultsRecord,
    ) -> Result<Vec<ExpectedReturnAvailability>, DecisionApplicationError> {
        record.validate_authority(&self.reader()?.authority)
    }
    pub(crate) fn publish_find_results(
        &self,
        parent: &CurrentFindPreparationRecord,
        screen_job: Option<FindScreenJobReference>,
        analyses: Vec<FindAnalysisReference>,
        context: &RequestContext,
    ) -> Result<FindResultsRecord, ServiceError> {
        parent.authorize(context)?;
        if analyses.len() > parent.population.maximum_deep_analyses() {
            return Err(ServiceError::InvalidRequest);
        }
        let mut state = self.writer().map_err(map)?;
        check_live(context)?;
        let Some(CurrentFindCustodyRecord::Preparation(actual)) = state
            .journal
            .current_find_record(&format!(
                "current-find:preparation:{}",
                parent.preparation_id
            ))
            .map_err(map)?
        else {
            return Err(ServiceError::InvalidResult);
        };
        if *actual != *parent {
            return Err(ServiceError::InvalidResult);
        }
        if let Some(CurrentFindCustodyRecord::Results(record)) = state
            .journal
            .current_find_record(&key(parent.preparation_id))
            .map_err(map)?
        {
            if record.preparation_sha256 != parent.digest().map_err(map)?
                || record.screen_job != screen_job
                || !record.rows.iter().map(|r| &r.reference).eq(analyses.iter())
            {
                return Err(ServiceError::InvalidRequest);
            }
            record.validate_authority(&state.authority).map_err(map)?;
            return Ok(*record);
        }
        let screen = state
            .journal
            .current_find_record(&format!("current-find:screen:{}", parent.preparation_id))
            .map_err(map)?;
        let run_id = match screen {
            Some(CurrentFindCustodyRecord::Screen(screen)) if screen_job.is_some() => {
                Some(screen.run_id.clone())
            }
            None if screen_job.is_none() => None,
            _ => return Err(ServiceError::InvalidRequest),
        };
        let mut rows = Vec::new();
        let mut estimates = Vec::new();
        rows.try_reserve_exact(analyses.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        estimates
            .try_reserve_exact(analyses.len())
            .map_err(|_| ServiceError::ResourceExhausted)?;
        let mut coordinate = None;
        let mut comparable = true;
        for reference in analyses {
            if let Some(expected) = &reference.unavailable_assessment_sha256 {
                if reference.action_token.is_some() {
                    return Err(ServiceError::InvalidRequest);
                }
                let member = super::current_find::member::FindMemberContext {
                    preparation_id: parent.preparation_id,
                    preparation_sha256: crate::application::model::forecast_preparation::hex(
                        market_squawk_data::Sha256Digest::new(parent.digest().map_err(map)?),
                    ),
                    candidate_id: reference.candidate_id.clone(),
                    screen_run_id: run_id.clone().ok_or(ServiceError::InvalidRequest)?,
                };
                let Some(CurrentFindCustodyRecord::MemberUnavailable(saved)) = state
                    .journal
                    .current_find_record(&super::current_find::member::key(&member))
                    .map_err(map)?
                else {
                    return Err(ServiceError::InvalidRequest);
                };
                if saved.member != member
                    || saved.value().map_err(map)?.assessment_sha256 != *expected
                {
                    return Err(ServiceError::InvalidRequest);
                }
                estimates.push(
                    ExpectedReturnAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied,
                );
                rows.push(FindResultRow {
                    reference,
                    analysis_id: None,
                    outcome_digest: None,
                    member_unavailable: Some(saved.value().map_err(map)?),
                });
                continue;
            }
            let action = reference.action_token.ok_or(ServiceError::InvalidRequest)?;
            let mut matched = state
                .authority
                .repository()
                .investment_proposals()
                .filter(|d| super::investment_analysis_product_token(d.analysis_id()) == action);
            let decision = matched.next().ok_or(ServiceError::NotFound)?;
            if matched.next().is_some() {
                return Err(ServiceError::InvalidResult);
            }
            let bundle = state
                .authority
                .get_prepared_published_investment_analysis(decision.analysis_id())
                .map_err(DecisionApplicationError::from)
                .map_err(map)?;
            let projection = decision.proposal_id().and_then(|id| {
                state
                    .authority
                    .repository()
                    .investment_outcome_projection(id)
            });
            if decision.proposal_id().is_some() && projection.is_none() {
                return Err(ServiceError::Unavailable);
            }
            estimates.push(projection.map_or(
                ExpectedReturnAvailability::UnavailableAdmittedExpectedTerminalValueNotSupplied,
                |p| p.expected_return(),
            ));
            if projection.is_some_and(|p| {
                matches!(
                    p.expected_return(),
                    ExpectedReturnAvailability::Available(_)
                )
            }) {
                let forecast = bundle
                    .decision()
                    .evidence()
                    .price_forecast()
                    .ok_or(ServiceError::InvalidResult)?;
                let current_coordinate = (
                    bundle.decision().policy().horizon_nanos(),
                    forecast.window().observed_at(),
                    forecast.horizon_at(),
                );
                if let Some(expected) = coordinate {
                    if expected != current_coordinate {
                        comparable = false;
                    }
                } else {
                    coordinate = Some(current_coordinate);
                }
            }
            rows.push(FindResultRow {
                reference,
                analysis_id: Some(decision.analysis_id().bytes()),
                member_unavailable: None,
                outcome_digest: projection.map(|p| p.result_digest().bytes()),
            });
        }
        let record = FindResultsRecord {
            preparation_id: parent.preparation_id,
            preparation_sha256: parent.digest().map_err(map)?,
            workspace_id: context
                .origin()
                .ok_or(ServiceError::Unauthorized)?
                .workspace_id(),
            profile_sha256: super::investment_request::parse_digest(
                &parent.profile.configuration_digest,
            )?
            .bytes(),
            screen_job,
            screen_run_id: run_id,
            comparable_horizons: comparable,
            ranked_indices: ranked(&estimates, comparable)
                .map_err(map)?
                .into_boxed_slice(),
            rows: rows.into_boxed_slice(),
        };
        record.validate_authority(&state.authority).map_err(map)?;
        // Reuse the complete parent/partition recovery invariant before append.
        let mut recovery = std::collections::BTreeMap::new();
        let mut source_pages = std::collections::BTreeMap::new();
        for page in &parent.source_pages {
            let original = state
                .journal
                .current_find_record(&page.key())
                .map_err(map)?
                .ok_or(ServiceError::InvalidResult)?;
            super::current_find::recover(&original, &mut recovery, &mut source_pages)
                .map_err(map)?;
        }
        super::current_find::recover(
            &CurrentFindCustodyRecord::Preparation(actual),
            &mut recovery,
            &mut source_pages,
        )
        .map_err(map)?;
        for ordinal in 0..parent.partition_count {
            for kind in ["partition", "completion"] {
                let original = state
                    .journal
                    .current_find_record(&format!(
                        "current-find:{kind}:{}:{ordinal}",
                        parent.preparation_id
                    ))
                    .map_err(map)?
                    .ok_or(ServiceError::InvalidRequest)?;
                super::current_find::recover(&original, &mut recovery, &mut source_pages)
                    .map_err(map)?;
            }
        }
        if let Some(screen) = state
            .journal
            .current_find_record(&format!("current-find:screen:{}", parent.preparation_id))
            .map_err(map)?
        {
            super::current_find::recover(&screen, &mut recovery, &mut source_pages).map_err(map)?;
        }
        for row in &record.rows {
            // Load even for analyzed rows, so an already terminal unavailable member cannot be
            // replaced by a later analysis and silently lose its original assessment.
            if let Some(run_id) = &record.screen_run_id {
                let member = super::current_find::member::FindMemberContext {
                    preparation_id: parent.preparation_id,
                    preparation_sha256: crate::application::model::forecast_preparation::hex(
                        market_squawk_data::Sha256Digest::new(parent.digest().map_err(map)?),
                    ),
                    candidate_id: row.reference.candidate_id.clone(),
                    screen_run_id: run_id.clone(),
                };
                if let Some(original) = state
                    .journal
                    .current_find_record(&super::current_find::member::key(&member))
                    .map_err(map)?
                {
                    super::current_find::recover(&original, &mut recovery, &mut source_pages)
                        .map_err(map)?;
                }
            }
        }
        let custody = CurrentFindCustodyRecord::Results(Box::new(record.clone()));
        super::current_find::recover(&custody, &mut recovery, &mut source_pages).map_err(map)?;
        let encoded = super::codec::current_find_custody(&custody).map_err(map)?;
        check_live(context)?;
        if let Err(error) = state.journal.append(&encoded) {
            state.poisoned = true;
            return Err(map(error));
        }
        Ok(record)
    }
}
fn key(id: Uuid) -> String {
    format!("current-find:results:{id}")
}
fn invalid() -> DecisionApplicationError {
    DecisionApplicationError::InvalidPersistentState
}
fn map(error: DecisionApplicationError) -> ServiceError {
    use market_squawk_decisions::DecisionRepositoryError as R;
    match error {
        DecisionApplicationError::Repository(R::NotFound) => ServiceError::NotFound,
        DecisionApplicationError::Repository(R::Capacity | R::Allocation)
        | DecisionApplicationError::Allocation
        | DecisionApplicationError::Capacity => ServiceError::ResourceExhausted,
        DecisionApplicationError::Repository(_) => ServiceError::InvalidRequest,
        DecisionApplicationError::Unavailable | DecisionApplicationError::Persistence => {
            ServiceError::Unavailable
        }
        DecisionApplicationError::InvalidPersistentState => ServiceError::InvalidResult,
    }
}
fn check_live(context: &RequestContext) -> Result<(), ServiceError> {
    if context.cancellation().is_cancelled() {
        return Err(ServiceError::Cancelled);
    }
    if std::time::Instant::now() >= context.deadline() {
        return Err(ServiceError::DeadlineExceeded);
    }
    Ok(())
}
