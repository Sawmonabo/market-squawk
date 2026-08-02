//! Conversion of bounded parser drafts into exact occurrence families.

use std::collections::BTreeMap;
use std::str::FromStr as _;

use market_squawk_domain::{
    SourceIdentifier, XbrlDuplicateClass, XbrlDuplicateEvidence, XbrlEntity, XbrlFactEvidenceInput,
    XbrlOccurrenceRelationships, XbrlSign, XbrlText, XbrlUnitExpression,
};
use rust_decimal::Decimal;

use super::*;

pub(super) enum NormalizedDraft {
    Numeric {
        concept: SourceIdentifier,
        unit: SourceIdentifier,
        value: Decimal,
        evidence_input: Box<XbrlFactEvidenceInput>,
    },
    Nonnumeric(Box<XbrlNonnumericOccurrence>),
}

impl NormalizedDraft {
    pub(super) fn try_new(
        fact: FactDraft,
        contexts: &BTreeMap<String, ContextDraft>,
        units: &BTreeMap<String, XbrlUnitExpression>,
        occurrence_graph: &BTreeMap<String, XbrlOccurrenceRelationships>,
        document: &XbrlDocumentContext,
    ) -> Result<Self, SecXbrlError> {
        let context = contexts
            .get(&fact.context_id)
            .ok_or(SecXbrlError::UnknownContext)?;
        let occurrence_id = SourceIdentifier::try_from(fact.occurrence_id.clone())?;
        let source_concept = fact.concept.source_qname().clone();
        let context_id = SourceIdentifier::try_from(fact.context_id)?;
        let exact_text = XbrlText::try_from(fact.text.trim().to_owned())?;
        let relationships = occurrence_graph
            .get(&fact.occurrence_id)
            .cloned()
            .ok_or(SecXbrlError::ParserInvariant)?;
        if fact.nil || fact.explicitly_nonnumeric || fact.unit_id.is_none() {
            return Ok(Self::Nonnumeric(Box::new(XbrlNonnumericOccurrence {
                occurrence_id,
                accession: document.accession.clone(),
                concept: fact.concept,
                context_id,
                lexical_value: exact_text,
                nil: fact.nil,
                source_payload: document.source_payload.clone(),
                occurrence_relationships: relationships,
            })));
        }
        let unit_id_text = fact.unit_id.ok_or(SecXbrlError::UnknownUnit)?;
        let unit_expression = units
            .get(&unit_id_text)
            .cloned()
            .ok_or(SecXbrlError::UnknownUnit)?;
        let unit = unit_expression.source_identifier()?;
        let transformed = transform_numeric(exact_text.as_str(), fact.format.as_ref())?;
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
        Ok(Self::Numeric {
            concept: source_concept,
            unit,
            value: value.normalize(),
            evidence_input: Box::new(XbrlFactEvidenceInput {
                occurrence_id,
                accession: document.accession.clone(),
                context_id,
                unit_id: SourceIdentifier::try_from(unit_id_text)?,
                concept: fact.concept,
                unit: unit_expression,
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
                context_graph: market_squawk_domain::XbrlContextGraph::try_new(
                    context.graph_events.clone(),
                )?,
                occurrence_relationships: relationships,
                language: fact.language.map(SourceIdentifier::try_from).transpose()?,
                duplicate: XbrlDuplicateEvidence::try_new(
                    XbrlDuplicateClass::Unique,
                    None,
                    SourceIdentifier::try_from("sec-xbrl-duplicate-v2")?,
                )?,
                taxonomy_set: document.taxonomy_set.clone(),
                source_payload: document.source_payload.clone(),
                parser_ruleset: SourceIdentifier::try_from("sec-xbrl-parser-v2")?,
                rounding_ruleset: SourceIdentifier::try_from("sec-xbrl-rounding-v2")?,
                evaluated_at: document.evaluated_at,
            }),
        })
    }
}
