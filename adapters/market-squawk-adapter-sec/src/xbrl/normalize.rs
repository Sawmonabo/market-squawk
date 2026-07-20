//! Conversion of bounded parser drafts into exact occurrence families.

use std::collections::BTreeMap;
use std::str::FromStr as _;

use market_squawk_domain::{
    SourceIdentifier, XbrlDuplicateClass, XbrlDuplicateEvidence, XbrlEntity, XbrlFactEvidenceInput,
    XbrlSign, XbrlText,
};
use rust_decimal::Decimal;

use super::*;

pub(super) enum NormalizedDraft {
    Numeric {
        concept: SourceIdentifier,
        context_id: SourceIdentifier,
        unit: SourceIdentifier,
        value: Decimal,
        evidence_input: Box<XbrlFactEvidenceInput>,
    },
    Nonnumeric(XbrlNonnumericOccurrence),
}

impl NormalizedDraft {
    pub(super) fn try_new(
        fact: FactDraft,
        index: usize,
        contexts: &BTreeMap<String, ContextDraft>,
        units: &BTreeMap<String, String>,
        document: &XbrlDocumentContext,
    ) -> Result<Self, SecXbrlError> {
        let context = contexts
            .get(&fact.context_id)
            .ok_or(SecXbrlError::UnknownContext)?;
        let occurrence_id = SourceIdentifier::try_from(fact.occurrence_id.unwrap_or(format!(
            "{}-fact-{}",
            document.accession,
            index + 1
        )))?;
        let concept = SourceIdentifier::try_from(fact.concept)?;
        let context_id = SourceIdentifier::try_from(fact.context_id)?;
        let exact_text = XbrlText::try_from(fact.text.trim().to_owned())?;
        if fact.nil || fact.explicitly_nonnumeric || fact.unit_id.is_none() {
            return Ok(Self::Nonnumeric(XbrlNonnumericOccurrence {
                occurrence_id,
                accession: document.accession.clone(),
                concept,
                context_id,
                lexical_value: exact_text,
                nil: fact.nil,
                source_payload: document.source_payload.clone(),
            }));
        }
        let unit_id_text = fact.unit_id.ok_or(SecXbrlError::UnknownUnit)?;
        let measure = units.get(&unit_id_text).ok_or(SecXbrlError::UnknownUnit)?;
        let normalized_unit = measure
            .rsplit(':')
            .next()
            .ok_or(SecXbrlError::IncompleteUnit)?;
        let unit = SourceIdentifier::try_from(normalized_unit)?;
        let transformed = transform_numeric(exact_text.as_str(), fact.format.as_deref())?;
        let mut value =
            Decimal::from_str(&transformed).map_err(|_| SecXbrlError::InvalidNumericFact)?;
        let scale = fact.scale.unwrap_or(0);
        if !(-28..=28).contains(&scale) {
            return Err(SecXbrlError::InvalidNumericFact);
        }
        if scale >= 0 {
            for _ in 0..scale {
                value = value
                    .checked_mul(Decimal::TEN)
                    .ok_or(SecXbrlError::InvalidNumericFact)?;
            }
        } else {
            for _ in scale..0 {
                value = value
                    .checked_div(Decimal::TEN)
                    .ok_or(SecXbrlError::InvalidNumericFact)?;
            }
        }
        if fact.sign == Some(XbrlSign::Negative) {
            value = value
                .checked_mul(Decimal::NEGATIVE_ONE)
                .ok_or(SecXbrlError::InvalidNumericFact)?;
        }
        let segment = context.segment_text.trim();
        Ok(Self::Numeric {
            concept,
            context_id: context_id.clone(),
            unit,
            value: value.normalize(),
            evidence_input: Box::new(XbrlFactEvidenceInput {
                occurrence_id,
                accession: document.accession.clone(),
                context_id,
                unit_id: SourceIdentifier::try_from(unit_id_text)?,
                entity: XbrlEntity::try_new(
                    context
                        .entity_scheme
                        .as_deref()
                        .ok_or(SecXbrlError::IncompleteContext)?,
                    context
                        .entity_value
                        .as_deref()
                        .ok_or(SecXbrlError::IncompleteContext)?,
                )?,
                period: context.period()?,
                accuracy: fact.accuracy,
                lexical_value: exact_text,
                transformed_lexeme: Some(XbrlText::try_from(transformed)?),
                inline_scale: fact.scale,
                inline_sign: fact.sign,
                dimensions: context.dimensions.clone(),
                segment_evidence: if segment.is_empty() {
                    None
                } else {
                    Some(XbrlText::try_from(segment)?)
                },
                language: fact.language.map(SourceIdentifier::try_from).transpose()?,
                duplicate: XbrlDuplicateEvidence::try_new(
                    XbrlDuplicateClass::Unique,
                    None,
                    SourceIdentifier::try_from("sec-xbrl-duplicate-v1")?,
                )?,
                taxonomy_set: document.taxonomy_set.clone(),
                source_payload: document.source_payload.clone(),
                parser_ruleset: SourceIdentifier::try_from("sec-xbrl-parser-v1")?,
                rounding_ruleset: SourceIdentifier::try_from("sec-xbrl-rounding-v1")?,
                evaluated_at: document.evaluated_at,
            }),
        })
    }
}
