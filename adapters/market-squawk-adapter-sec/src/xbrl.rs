//! Bounded namespace-authoritative parser for XBRL and Inline XBRL occurrences.

mod model;
mod normalize;
mod support;
mod wire;

use std::collections::{BTreeMap, BTreeSet};
use std::mem::size_of;

use crate::SecParserLimits;
use market_squawk_domain::{
    CalendarDate, MAX_XBRL_GRAPH_EVENTS, SourceIdentifier, XbrlAccuracy, XbrlAccuracyValue,
    XbrlContextGraph, XbrlDimensionEvidence, XbrlDimensionLocation, XbrlDimensionMember,
    XbrlDuplicateClass, XbrlDuplicateEvidence, XbrlFactEvidence, XbrlFactEvidenceInput,
    XbrlOccurrenceRelationships, XbrlPeriod, XbrlQualifiedName, XbrlRelationshipEvidence, XbrlSign,
    XbrlText, XbrlTypedMemberValidation, XbrlUnitExpression, XbrlXmlEvent,
};
use quick_xml::NsReader;
use quick_xml::events::Event;
use quick_xml::name::NamespaceResolver;
use rust_decimal::Decimal;
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

pub(crate) use model::SecPendingValidatedXbrlTaxonomySet;
pub use model::{
    ParsedXbrlDocument, SecValidatedXbrlTaxonomySet, SecXbrlTaxonomyRegistry, XbrlDocumentContext,
    XbrlNonnumericOccurrence, XbrlNumericFact,
};
use normalize::NormalizedDraft;
pub use support::SecXbrlError;
use wire::*;

/// Every prospective charge includes a twofold allowance for collection growth, string spare
/// capacity, and allocator size-class rounding on the pinned production toolchain. Charges are
/// cumulative and are never refunded when an allocation is moved or released, so the admitted
/// total conservatively bounds every simultaneously live parser-draft and output allocation.
const RETAINED_CAPACITY_ALLOWANCE: usize = 2;
const BTREE_LINK_WORDS_PER_ENTRY: usize = 4;

#[derive(Debug)]
struct RetainedOutputBudget {
    admitted: usize,
    limit: usize,
}

impl RetainedOutputBudget {
    const fn new(limit: usize) -> Self {
        Self { admitted: 0, limit }
    }

    fn admit(&mut self, language_visible_bytes: usize) -> Result<(), SecXbrlError> {
        let conservative = language_visible_bytes
            .checked_mul(RETAINED_CAPACITY_ALLOWANCE)
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
        let admitted = self
            .admitted
            .checked_add(conservative)
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
        if admitted > self.limit {
            return Err(SecXbrlError::RetainedOutputLimitExceeded);
        }
        self.admitted = admitted;
        Ok(())
    }

    fn admit_vec_entry<T>(&mut self, dynamic_bytes: usize) -> Result<(), SecXbrlError> {
        self.admit(
            size_of::<T>()
                .checked_add(dynamic_bytes)
                .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
        )
    }

    fn admit_btree_entry<K, V>(&mut self, dynamic_bytes: usize) -> Result<(), SecXbrlError> {
        let inline = size_of::<K>()
            .checked_add(size_of::<V>())
            .and_then(|bytes| {
                bytes.checked_add(BTREE_LINK_WORDS_PER_ENTRY.checked_mul(size_of::<usize>())?)
            })
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
        self.admit(
            inline
                .checked_add(dynamic_bytes)
                .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
        )
    }

    const fn admitted(&self) -> usize {
        self.admitted
    }
}

fn checked_retained_sum(values: impl IntoIterator<Item = usize>) -> Result<usize, SecXbrlError> {
    values.into_iter().try_fold(0usize, |total, value| {
        total
            .checked_add(value)
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
    })
}

fn qname_dynamic_bytes(name: &XbrlQualifiedName) -> Result<usize, SecXbrlError> {
    checked_retained_sum([
        name.source_qname().retained_bytes(),
        name.local_name().retained_bytes(),
        name.namespace_uri().map_or(0, XbrlText::retained_bytes),
    ])
}

fn xml_event_dynamic_bytes(event: &XbrlXmlEvent) -> Result<usize, SecXbrlError> {
    match event {
        XbrlXmlEvent::Start { name } | XbrlXmlEvent::End { name } => qname_dynamic_bytes(name),
        XbrlXmlEvent::Attribute { name, value } => {
            checked_retained_sum([qname_dynamic_bytes(name)?, value.retained_bytes()])
        }
        XbrlXmlEvent::Text { value } => Ok(value.retained_bytes()),
    }
}

fn graph_dynamic_bytes(graph: &XbrlContextGraph) -> Result<usize, SecXbrlError> {
    let slots = graph
        .events()
        .len()
        .checked_mul(size_of::<XbrlXmlEvent>())
        .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
    graph.events().iter().try_fold(slots, |total, event| {
        total
            .checked_add(xml_event_dynamic_bytes(event)?)
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
    })
}

fn dimension_dynamic_bytes(dimension: &XbrlDimensionEvidence) -> Result<usize, SecXbrlError> {
    let member = match dimension.member() {
        XbrlDimensionMember::Explicit { member } => qname_dynamic_bytes(member)?,
        XbrlDimensionMember::Typed { source_graph, .. } => graph_dynamic_bytes(source_graph)?,
    };
    checked_retained_sum([qname_dynamic_bytes(dimension.dimension())?, member])
}

fn unit_dynamic_bytes(unit: &XbrlUnitExpression) -> Result<usize, SecXbrlError> {
    if let Some(measure) = unit.measure_name() {
        return qname_dynamic_bytes(measure);
    }
    let Some((numerator, denominator)) = unit.divide_parts() else {
        return Err(SecXbrlError::ParserInvariant);
    };
    numerator.iter().chain(denominator).try_fold(
        numerator
            .len()
            .checked_add(denominator.len())
            .and_then(|length| length.checked_mul(size_of::<XbrlQualifiedName>()))
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
        |total, name| {
            total
                .checked_add(qname_dynamic_bytes(name)?)
                .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
        },
    )
}

/// Bounded XBRL/Inline-XBRL parser.
#[derive(Clone, Copy, Debug, Default)]
pub struct XbrlDocumentParser;

impl XbrlDocumentParser {
    /// Consumes one exact document context while parsing with cooperative per-event cancellation.
    pub fn parse_with_cancellation(
        bytes: &[u8],
        limits: SecParserLimits,
        document: XbrlDocumentContext,
        cancellation: &CancellationToken,
    ) -> Result<ParsedXbrlDocument, SecXbrlError> {
        if bytes.len() > limits.decoded_bytes() {
            return Err(SecXbrlError::ByteLimitExceeded);
        }
        let mut reader = NsReader::from_reader(bytes);
        reader.config_mut().trim_text(false);
        reader.config_mut().expand_empty_elements = true;
        let mut state = ParserState::new(limits, document);
        loop {
            check_xbrl_cancelled(cancellation)?;
            let (resolution, event) = reader.read_resolved_event()?;
            match event {
                Event::Start(start) => {
                    let name = resolve_element_name(resolution, start.name(), limits)?;
                    let attributes = attributes(&reader, &start, limits)?;
                    state.start(reader.resolver(), name, attributes)?;
                }
                Event::End(end) => {
                    let name = resolve_element_name(resolution, end.name(), limits)?;
                    state.end(reader.resolver(), &name)?;
                }
                Event::Text(text) => {
                    let decoded = text.xml10_content()?;
                    let unescaped = quick_xml::escape::unescape(&decoded)?;
                    state.text(&unescaped)?;
                }
                Event::CData(text) => state.text(&text.decode()?)?,
                Event::DocType(_) => return Err(SecXbrlError::DoctypeForbidden),
                Event::Eof => break,
                Event::Decl(_) | Event::PI(_) | Event::Comment(_) | Event::GeneralRef(_) => {}
                Event::Empty(_) => return Err(SecXbrlError::ParserInvariant),
            }
        }
        state.finish(cancellation)
    }
}

fn check_xbrl_cancelled(cancellation: &CancellationToken) -> Result<(), SecXbrlError> {
    if cancellation.is_cancelled() {
        Err(SecXbrlError::Cancelled)
    } else {
        Ok(())
    }
}

struct ParserState {
    limits: SecParserLimits,
    retained_output: RetainedOutputBudget,
    document: XbrlDocumentContext,
    depth: usize,
    contexts: BTreeMap<String, ContextDraft>,
    units: BTreeMap<String, XbrlUnitExpression>,
    current_context: Option<ContextDraft>,
    current_unit: Option<UnitDraft>,
    context_container: Option<(usize, XbrlDimensionLocation)>,
    capture: Option<Capture>,
    active_facts: Vec<FactDraft>,
    active_continuation: Option<ContinuationDraft>,
    continuations: BTreeMap<String, ContinuationDraft>,
    exclude_depth: Option<usize>,
    facts: Vec<FactDraft>,
    relationships: Vec<RelationshipDraft>,
    next_fact_ordinal: usize,
}

impl ParserState {
    fn new(limits: SecParserLimits, document: XbrlDocumentContext) -> Self {
        Self {
            retained_output: RetainedOutputBudget::new(limits.retained_output_bytes()),
            limits,
            document,
            depth: 0,
            contexts: BTreeMap::new(),
            units: BTreeMap::new(),
            current_context: None,
            current_unit: None,
            context_container: None,
            capture: None,
            active_facts: Vec::new(),
            active_continuation: None,
            continuations: BTreeMap::new(),
            exclude_depth: None,
            facts: Vec::new(),
            relationships: Vec::new(),
            next_fact_ordinal: 0,
        }
    }

    fn start(
        &mut self,
        resolver: &NamespaceResolver,
        name: XbrlQualifiedName,
        attributes: ResolvedAttributes,
    ) -> Result<(), SecXbrlError> {
        self.depth = self
            .depth
            .checked_add(1)
            .ok_or(SecXbrlError::DepthLimitExceeded)?;
        if self.depth > self.limits.depth() {
            return Err(SecXbrlError::DepthLimitExceeded);
        }

        let container_location = if is_element(&name, XBRLI_NAMESPACE, "segment") {
            Some(XbrlDimensionLocation::Segment)
        } else if is_element(&name, XBRLI_NAMESPACE, "scenario") {
            Some(XbrlDimensionLocation::Scenario)
        } else {
            None
        };
        if self.current_context.is_some()
            && (self.context_container.is_some() || container_location.is_some())
        {
            self.append_context_start(name.clone(), &attributes)?;
        }

        if is_element(&name, IX_NAMESPACE, "continuation") {
            if self.active_continuation.is_some() || !self.active_facts.is_empty() {
                return Err(SecXbrlError::NestedContinuation);
            }
            let id = attributes
                .unqualified("id")
                .ok_or(SecXbrlError::MissingAttribute)?;
            let continued_at = attributes.unqualified("continuedAt");
            self.retained_output.admit(checked_retained_sum([
                id.len(),
                continued_at.map_or(0, str::len),
            ])?)?;
            self.active_continuation = Some(ContinuationDraft {
                start_depth: self.depth,
                id: id.to_owned(),
                continued_at: continued_at.map(str::to_owned),
                text: String::new(),
            });
            return Ok(());
        }
        if is_element(&name, IX_NAMESPACE, "exclude") {
            if self.exclude_depth.is_some() {
                return Err(SecXbrlError::NestedExclude);
            }
            self.exclude_depth = Some(self.depth);
            return Ok(());
        }
        if is_element(&name, IX_NAMESPACE, "relationship") {
            self.retained_output.admit_vec_entry::<RelationshipDraft>(
                RelationshipDraft::dynamic_bytes_from_attributes(&attributes)?,
            )?;
            self.relationships
                .push(RelationshipDraft::try_new(&attributes)?);
            if self.relationships.len() > self.limits.records() {
                return Err(SecXbrlError::RecordLimitExceeded);
            }
            return Ok(());
        }
        if is_element(&name, XBRLI_NAMESPACE, "context") {
            if self.current_context.is_some() {
                return Err(SecXbrlError::NestedContext);
            }
            let id = attributes
                .unqualified("id")
                .ok_or(SecXbrlError::MissingAttribute)?;
            self.retained_output.admit(id.len())?;
            self.current_context = Some(ContextDraft::new(id.to_owned()));
            return Ok(());
        }
        if is_element(&name, XBRLI_NAMESPACE, "unit") {
            if self.current_unit.is_some() {
                return Err(SecXbrlError::NestedUnit);
            }
            let id = attributes
                .unqualified("id")
                .ok_or(SecXbrlError::MissingAttribute)?;
            self.retained_output.admit(id.len())?;
            self.current_unit = Some(UnitDraft::new(id.to_owned()));
            return Ok(());
        }
        if self.current_context.is_some() {
            if let Some(location) = container_location {
                if self.context_container.is_some() {
                    return Err(SecXbrlError::IncompleteContext);
                }
                self.context_container = Some((self.depth, location));
            } else if is_element(&name, XBRLI_NAMESPACE, "identifier") {
                self.begin_capture(CaptureKind::Identifier {
                    scheme: attributes.required_unqualified("scheme")?,
                })?;
            } else if is_element(&name, XBRLI_NAMESPACE, "instant") {
                self.begin_capture(CaptureKind::Instant)?;
            } else if is_element(&name, XBRLI_NAMESPACE, "startDate") {
                self.begin_capture(CaptureKind::StartDate)?;
            } else if is_element(&name, XBRLI_NAMESPACE, "endDate") {
                self.begin_capture(CaptureKind::EndDate)?;
            } else if is_element(&name, XBRLDI_NAMESPACE, "explicitMember") {
                self.begin_capture(CaptureKind::ExplicitMember {
                    dimension: resolve_qname_value(
                        resolver,
                        &attributes.required_unqualified("dimension")?,
                        self.limits,
                    )?,
                    location: self.context_location()?,
                })?;
            } else if is_element(&name, XBRLDI_NAMESPACE, "typedMember") {
                let graph_start = self
                    .current_context
                    .as_ref()
                    .ok_or(SecXbrlError::ParserInvariant)?
                    .graph_events
                    .len();
                self.begin_capture(CaptureKind::TypedMember {
                    dimension: resolve_qname_value(
                        resolver,
                        &attributes.required_unqualified("dimension")?,
                        self.limits,
                    )?,
                    location: self.context_location()?,
                    graph_start,
                })?;
            }
            return Ok(());
        }
        if self.current_unit.is_some() {
            if is_element(&name, XBRLI_NAMESPACE, "divide") {
                self.current_unit_mut()?.start_divide()?;
            } else if is_element(&name, XBRLI_NAMESPACE, "unitNumerator") {
                self.current_unit_mut()?.start_side(UnitSide::Numerator)?;
            } else if is_element(&name, XBRLI_NAMESPACE, "unitDenominator") {
                self.current_unit_mut()?.start_side(UnitSide::Denominator)?;
            } else if is_element(&name, XBRLI_NAMESPACE, "measure") {
                self.begin_capture(CaptureKind::Measure)?;
            }
            return Ok(());
        }

        let inline_numeric = is_element(&name, IX_NAMESPACE, "nonFraction");
        let inline_nonnumeric = is_element(&name, IX_NAMESPACE, "nonNumeric");
        let context_ref = attributes.unqualified("contextRef");
        if inline_numeric || inline_nonnumeric || context_ref.is_some() {
            if !inline_numeric && !inline_nonnumeric && name.namespace_uri().is_none() {
                return Err(SecXbrlError::UnknownNamespacePrefix);
            }
            if !self.active_facts.is_empty() && !inline_numeric && !inline_nonnumeric {
                return Err(SecXbrlError::NestedFact);
            }
            self.next_fact_ordinal = self
                .next_fact_ordinal
                .checked_add(1)
                .ok_or(SecXbrlError::RecordLimitExceeded)?;
            if self.next_fact_ordinal > self.limits.records() {
                return Err(SecXbrlError::RecordLimitExceeded);
            }
            let occurrence_id = attributes
                .unqualified("id")
                .map(str::to_owned)
                .unwrap_or_else(|| {
                    format!(
                        "{}-fact-{}",
                        self.document.accession, self.next_fact_ordinal
                    )
                });
            let concept = if inline_numeric || inline_nonnumeric {
                resolve_qname_value(
                    resolver,
                    &attributes.required_unqualified("name")?,
                    self.limits,
                )?
            } else {
                name
            };
            let format = attributes
                .unqualified("format")
                .map(|value| resolve_qname_value(resolver, value, self.limits))
                .transpose()?;
            let parent_occurrence_id = self
                .active_facts
                .last()
                .map(|parent| parent.occurrence_id.as_str());
            let fact_dynamic = checked_retained_sum([
                qname_dynamic_bytes(&concept)?,
                context_ref.map_or(0, str::len),
                attributes.unqualified("unitRef").map_or(0, str::len),
                occurrence_id.len(),
                parent_occurrence_id.map_or(0, str::len),
                format
                    .as_ref()
                    .map(qname_dynamic_bytes)
                    .transpose()?
                    .unwrap_or(0),
                attributes.xml_or_unqualified("lang").map_or(0, str::len),
                attributes.unqualified("continuedAt").map_or(0, str::len),
            ])?;
            self.retained_output
                .admit_vec_entry::<FactDraft>(fact_dynamic)?;
            self.active_facts.push(FactDraft {
                start_depth: self.depth,
                concept,
                context_id: context_ref
                    .map(str::to_owned)
                    .ok_or(SecXbrlError::MissingAttribute)?,
                unit_id: attributes.unqualified("unitRef").map(str::to_owned),
                occurrence_id,
                parent_occurrence_id: parent_occurrence_id.map(str::to_owned),
                accuracy: parse_accuracy(&attributes)?,
                scale: attributes.unqualified("scale").map(parse_i32).transpose()?,
                sign: attributes.unqualified("sign").map(parse_sign).transpose()?,
                format,
                language: attributes.xml_or_unqualified("lang").map(str::to_owned),
                nil: attributes.xsi_nil().is_some_and(is_true),
                explicitly_nonnumeric: inline_nonnumeric,
                continued_at: attributes.unqualified("continuedAt").map(str::to_owned),
                continuation_chain: Vec::new(),
                text: String::new(),
            });
        }
        Ok(())
    }

    fn current_context_mut(&mut self) -> Result<&mut ContextDraft, SecXbrlError> {
        self.current_context
            .as_mut()
            .ok_or(SecXbrlError::ParserInvariant)
    }

    fn current_unit_mut(&mut self) -> Result<&mut UnitDraft, SecXbrlError> {
        self.current_unit
            .as_mut()
            .ok_or(SecXbrlError::ParserInvariant)
    }

    fn context_location(&self) -> Result<XbrlDimensionLocation, SecXbrlError> {
        self.context_container
            .map(|(_, location)| location)
            .ok_or(SecXbrlError::IncompleteContext)
    }

    fn append_context_start(
        &mut self,
        name: XbrlQualifiedName,
        attributes: &ResolvedAttributes,
    ) -> Result<(), SecXbrlError> {
        let additional = attributes
            .values()
            .len()
            .checked_add(1)
            .ok_or(SecXbrlError::RecordLimitExceeded)?;
        let context = self.current_context_mut()?;
        if context
            .graph_events
            .len()
            .checked_add(additional)
            .is_none_or(|length| length > MAX_XBRL_GRAPH_EVENTS)
        {
            return Err(market_squawk_domain::XbrlEvidenceError::TooManyGraphEvents.into());
        }
        let mut retained = size_of::<XbrlXmlEvent>()
            .checked_add(qname_dynamic_bytes(&name)?)
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
        for attribute in attributes.values() {
            retained = retained
                .checked_add(size_of::<XbrlXmlEvent>())
                .and_then(|bytes| bytes.checked_add(qname_dynamic_bytes(&attribute.name).ok()?))
                .and_then(|bytes| bytes.checked_add(attribute.value.len()))
                .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
        }
        self.retained_output.admit(retained)?;
        let context = self.current_context_mut()?;
        context.graph_events.push(XbrlXmlEvent::Start { name });
        for attribute in attributes.values() {
            context.graph_events.push(XbrlXmlEvent::Attribute {
                name: attribute.name.clone(),
                value: XbrlText::try_from(attribute.value.clone())?,
            });
        }
        Ok(())
    }

    fn begin_capture(&mut self, kind: CaptureKind) -> Result<(), SecXbrlError> {
        if self.capture.is_some() {
            return Err(SecXbrlError::NestedCapture);
        }
        self.retained_output.admit(kind.dynamic_bytes()?)?;
        self.capture = Some(Capture {
            depth: self.depth,
            kind,
            text: String::new(),
        });
        Ok(())
    }

    fn text(&mut self, text: &str) -> Result<(), SecXbrlError> {
        if text.len() > self.limits.string_bytes() {
            return Err(SecXbrlError::StringLimitExceeded);
        }
        if self.exclude_depth.is_none() {
            let retained_copies = self
                .active_facts
                .len()
                .checked_add(if self.active_continuation.is_some() {
                    1
                } else {
                    0
                })
                .and_then(|copies| copies.checked_mul(text.len()))
                .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
            self.retained_output.admit(retained_copies)?;
            for fact in &mut self.active_facts {
                append_bounded(&mut fact.text, text, self.limits.string_bytes())?;
            }
            if let Some(continuation) = &mut self.active_continuation {
                append_bounded(&mut continuation.text, text, self.limits.string_bytes())?;
            }
        }
        if let Some(capture) = &mut self.capture {
            self.retained_output.admit(text.len())?;
            append_bounded(&mut capture.text, text, self.limits.string_bytes())?;
        }
        if self.context_container.is_some() {
            self.append_context_text(text)?;
        }
        Ok(())
    }

    fn append_context_text(&mut self, text: &str) -> Result<(), SecXbrlError> {
        if text.is_empty() {
            return Ok(());
        }
        let limit = self.limits.string_bytes();
        let previous_text_bytes = self
            .current_context
            .as_ref()
            .and_then(|context| context.graph_events.last())
            .and_then(|event| match event {
                XbrlXmlEvent::Text { value } => Some(value.as_str().len()),
                _ => None,
            });
        if let Some(previous_text_bytes) = previous_text_bytes {
            self.retained_output.admit(
                previous_text_bytes
                    .checked_add(text.len())
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
            )?;
            let context = self.current_context_mut()?;
            let Some(XbrlXmlEvent::Text { value }) = context.graph_events.last_mut() else {
                return Err(SecXbrlError::ParserInvariant);
            };
            let mut combined = value.as_str().to_owned();
            append_bounded(&mut combined, text, limit)?;
            *value = XbrlText::try_from(combined)?;
            return Ok(());
        }
        if self
            .current_context
            .as_ref()
            .is_none_or(|context| context.graph_events.len() >= MAX_XBRL_GRAPH_EVENTS)
        {
            return Err(market_squawk_domain::XbrlEvidenceError::TooManyGraphEvents.into());
        }
        self.retained_output
            .admit_vec_entry::<XbrlXmlEvent>(text.len())?;
        let context = self.current_context_mut()?;
        context.graph_events.push(XbrlXmlEvent::Text {
            value: XbrlText::try_from(text)?,
        });
        Ok(())
    }

    fn end(
        &mut self,
        resolver: &NamespaceResolver,
        name: &XbrlQualifiedName,
    ) -> Result<(), SecXbrlError> {
        if self.depth == 0 {
            return Err(SecXbrlError::ParserInvariant);
        }
        if self.context_container.is_some() {
            if self
                .current_context
                .as_ref()
                .is_none_or(|context| context.graph_events.len() >= MAX_XBRL_GRAPH_EVENTS)
            {
                return Err(market_squawk_domain::XbrlEvidenceError::TooManyGraphEvents.into());
            }
            self.retained_output
                .admit_vec_entry::<XbrlXmlEvent>(qname_dynamic_bytes(name)?)?;
            let context = self.current_context_mut()?;
            context
                .graph_events
                .push(XbrlXmlEvent::End { name: name.clone() });
        }
        if self
            .active_continuation
            .as_ref()
            .is_some_and(|continuation| continuation.start_depth == self.depth)
        {
            let continuation = self
                .active_continuation
                .take()
                .ok_or(SecXbrlError::ParserInvariant)?;
            self.retained_output
                .admit_btree_entry::<String, ContinuationDraft>(continuation.id.len())?;
            if self
                .continuations
                .insert(continuation.id.clone(), continuation)
                .is_some()
            {
                return Err(SecXbrlError::DuplicateIdentity);
            }
        }
        if self
            .active_facts
            .last()
            .is_some_and(|fact| fact.start_depth == self.depth)
        {
            let fact = self
                .active_facts
                .pop()
                .ok_or(SecXbrlError::ParserInvariant)?;
            self.retained_output.admit_vec_entry::<FactDraft>(0)?;
            self.facts.push(fact);
        }
        if self
            .capture
            .as_ref()
            .is_some_and(|capture| capture.depth == self.depth)
        {
            let capture = self.capture.take().ok_or(SecXbrlError::ParserInvariant)?;
            self.finish_capture(capture, resolver)?;
        }
        if is_element(name, XBRLI_NAMESPACE, "unitNumerator") {
            self.current_unit_mut()?.end_side(UnitSide::Numerator)?;
        }
        if is_element(name, XBRLI_NAMESPACE, "unitDenominator") {
            self.current_unit_mut()?.end_side(UnitSide::Denominator)?;
        }
        if is_element(name, XBRLI_NAMESPACE, "divide") {
            self.current_unit_mut()?.end_divide()?;
        }
        if self
            .context_container
            .is_some_and(|(depth, _)| depth == self.depth)
        {
            self.context_container = None;
        }
        if is_element(name, IX_NAMESPACE, "exclude") && self.exclude_depth == Some(self.depth) {
            self.exclude_depth = None;
        }
        if is_element(name, XBRLI_NAMESPACE, "context") {
            let context = self
                .current_context
                .take()
                .ok_or(SecXbrlError::ParserInvariant)?;
            self.retained_output
                .admit_btree_entry::<String, ContextDraft>(context.id.len())?;
            if self.contexts.insert(context.id.clone(), context).is_some() {
                return Err(SecXbrlError::DuplicateIdentity);
            }
        }
        if is_element(name, XBRLI_NAMESPACE, "unit") {
            let unit = self
                .current_unit
                .take()
                .ok_or(SecXbrlError::ParserInvariant)?;
            let id = unit.id.clone();
            let expression = unit.finish()?;
            self.retained_output
                .admit_btree_entry::<String, XbrlUnitExpression>(id.len())?;
            if self.units.insert(id, expression).is_some() {
                return Err(SecXbrlError::DuplicateIdentity);
            }
        }
        self.depth -= 1;
        Ok(())
    }

    fn finish_capture(
        &mut self,
        capture: Capture,
        resolver: &NamespaceResolver,
    ) -> Result<(), SecXbrlError> {
        let trimmed = capture.text.trim();
        self.retained_output.admit(trimmed.len())?;
        let text = trimmed.to_owned();
        match capture.kind {
            CaptureKind::Measure => {
                let measure = resolve_qname_value(resolver, &text, self.limits)?;
                self.retained_output
                    .admit_vec_entry::<XbrlQualifiedName>(qname_dynamic_bytes(&measure)?)?;
                self.current_unit_mut()?.push_measure(measure)?;
            }
            CaptureKind::Identifier { scheme } => {
                let context = self.current_context_mut()?;
                context.entity_scheme = Some(scheme);
                context.entity_value = Some(text);
            }
            CaptureKind::Instant => self.current_context_mut()?.instant = Some(parse_date(&text)?),
            CaptureKind::StartDate => self.current_context_mut()?.start = Some(parse_date(&text)?),
            CaptureKind::EndDate => self.current_context_mut()?.end = Some(parse_date(&text)?),
            CaptureKind::ExplicitMember {
                dimension,
                location,
            } => {
                let member = resolve_qname_value(resolver, &text, self.limits)?;
                let dimension_dynamic = checked_retained_sum([
                    qname_dynamic_bytes(&dimension)?,
                    qname_dynamic_bytes(&member)?,
                ])?;
                self.retained_output
                    .admit_vec_entry::<XbrlDimensionEvidence>(dimension_dynamic)?;
                self.current_context_mut()?
                    .dimensions
                    .push(XbrlDimensionEvidence::new(
                        dimension,
                        XbrlDimensionMember::Explicit { member },
                        location,
                    ));
            }
            CaptureKind::TypedMember {
                dimension,
                location,
                graph_start,
            } => {
                let context = self
                    .current_context
                    .as_ref()
                    .ok_or(SecXbrlError::ParserInvariant)?;
                let graph_end = context
                    .graph_events
                    .len()
                    .checked_sub(1)
                    .ok_or(SecXbrlError::ParserInvariant)?;
                let graph_events = context
                    .graph_events
                    .get(graph_start..graph_end)
                    .ok_or(SecXbrlError::ParserInvariant)?;
                let graph_dynamic = graph_events.iter().try_fold(
                    graph_events
                        .len()
                        .checked_mul(size_of::<XbrlXmlEvent>())
                        .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
                    |total, event| {
                        total
                            .checked_add(xml_event_dynamic_bytes(event)?)
                            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
                    },
                )?;
                self.retained_output.admit(
                    size_of::<XbrlDimensionEvidence>()
                        .checked_add(qname_dynamic_bytes(&dimension)?)
                        .and_then(|bytes| bytes.checked_add(graph_dynamic))
                        .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
                )?;
                let graph_events = graph_events.to_vec();
                self.current_context_mut()?
                    .dimensions
                    .push(XbrlDimensionEvidence::new(
                        dimension,
                        XbrlDimensionMember::Typed {
                            source_graph: XbrlContextGraph::try_new(graph_events)?,
                            validation: XbrlTypedMemberValidation::SourceOnly,
                        },
                        location,
                    ));
            }
        }
        Ok(())
    }

    fn finish(
        mut self,
        cancellation: &CancellationToken,
    ) -> Result<ParsedXbrlDocument, SecXbrlError> {
        check_xbrl_cancelled(cancellation)?;
        if self.depth != 0
            || self.current_context.is_some()
            || self.current_unit.is_some()
            || !self.active_facts.is_empty()
            || self.active_continuation.is_some()
            || self.capture.is_some()
            || self.exclude_depth.is_some()
            || self.context_container.is_some()
        {
            return Err(SecXbrlError::UnexpectedEof);
        }
        let mut occurrence_ids = BTreeSet::new();
        for fact in &self.facts {
            check_xbrl_cancelled(cancellation)?;
            self.retained_output
                .admit_btree_entry::<String, ()>(fact.occurrence_id.len())?;
            if !occurrence_ids.insert(fact.occurrence_id.clone()) {
                return Err(SecXbrlError::DuplicateIdentity);
            }
        }
        for fact in &mut self.facts {
            check_xbrl_cancelled(cancellation)?;
            let mut next = fact.continued_at.take();
            let mut seen = BTreeSet::new();
            while let Some(continuation_id) = next {
                check_xbrl_cancelled(cancellation)?;
                self.retained_output
                    .admit_btree_entry::<String, ()>(continuation_id.len())?;
                if !seen.insert(continuation_id.clone()) {
                    return Err(SecXbrlError::ContinuationCycle);
                }
                let continuation = self
                    .continuations
                    .get(&continuation_id)
                    .ok_or(SecXbrlError::UnknownContinuation)?;
                self.retained_output.admit(continuation.text.len())?;
                append_bounded(
                    &mut fact.text,
                    &continuation.text,
                    self.limits.string_bytes(),
                )?;
                self.retained_output.admit_vec_entry::<String>(0)?;
                fact.continuation_chain.push(continuation_id);
                self.retained_output.admit(
                    continuation
                        .continued_at
                        .as_ref()
                        .map_or(0, String::capacity),
                )?;
                next = continuation.continued_at.clone();
            }
        }
        let mut relationships = Vec::new();
        let mut relationship_clone_bytes = Vec::new();
        for relationship in self.relationships {
            check_xbrl_cancelled(cancellation)?;
            let clone_bytes = relationship.evidence_dynamic_bytes()?;
            self.retained_output
                .admit_vec_entry::<XbrlRelationshipEvidence>(clone_bytes)?;
            self.retained_output.admit_vec_entry::<usize>(0)?;
            relationships.push(relationship.into_evidence()?);
            relationship_clone_bytes.push(clone_bytes);
        }
        for relationship in &relationships {
            check_xbrl_cancelled(cancellation)?;
            if relationship
                .from_refs()
                .iter()
                .chain(relationship.to_refs())
                .any(|reference| !occurrence_ids.contains(reference.as_str()))
            {
                return Err(SecXbrlError::UnknownRelationshipReference);
            }
        }
        let mut children = BTreeMap::<String, Vec<SourceIdentifier>>::new();
        for fact in &self.facts {
            check_xbrl_cancelled(cancellation)?;
            if let Some(parent) = &fact.parent_occurrence_id {
                self.retained_output
                    .admit_vec_entry::<SourceIdentifier>(fact.occurrence_id.len())?;
                let child = SourceIdentifier::try_from(fact.occurrence_id.clone())?;
                if let Some(existing) = children.get_mut(parent) {
                    existing.push(child);
                } else {
                    self.retained_output
                        .admit_btree_entry::<String, Vec<SourceIdentifier>>(parent.len())?;
                    children.insert(parent.clone(), vec![child]);
                }
            }
        }
        let mut occurrence_graph = BTreeMap::new();
        let mut occurrence_graph_clone_bytes = BTreeMap::new();
        for fact in &self.facts {
            check_xbrl_cancelled(cancellation)?;
            let id = SourceIdentifier::try_from(fact.occurrence_id.clone())?;
            let mut incident = Vec::new();
            let mut incident_dynamic = 0usize;
            for (index, relationship) in relationships.iter().enumerate() {
                check_xbrl_cancelled(cancellation)?;
                if relationship.from_refs().contains(&id) || relationship.to_refs().contains(&id) {
                    let clone_bytes = *relationship_clone_bytes
                        .get(index)
                        .ok_or(SecXbrlError::ParserInvariant)?;
                    self.retained_output
                        .admit_vec_entry::<XbrlRelationshipEvidence>(clone_bytes)?;
                    incident_dynamic = incident_dynamic
                        .checked_add(size_of::<XbrlRelationshipEvidence>())
                        .and_then(|bytes| bytes.checked_add(clone_bytes))
                        .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
                    incident.push(relationship.clone());
                }
            }
            let parent_dynamic = fact
                .parent_occurrence_id
                .as_ref()
                .map_or(0, String::capacity);
            let child_occurrences = children.remove(&fact.occurrence_id).unwrap_or_default();
            let child_dynamic = child_occurrences.iter().try_fold(
                child_occurrences
                    .len()
                    .checked_mul(size_of::<SourceIdentifier>())
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
                |total, child| {
                    total
                        .checked_add(child.retained_bytes())
                        .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
                },
            )?;
            let continuation_dynamic = fact.continuation_chain.iter().try_fold(
                fact.continuation_chain
                    .len()
                    .checked_mul(size_of::<SourceIdentifier>())
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
                |total, continuation| {
                    total
                        .checked_add(continuation.capacity())
                        .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
                },
            )?;
            let graph_clone_bytes = checked_retained_sum([
                parent_dynamic,
                child_dynamic,
                continuation_dynamic,
                incident_dynamic,
            ])?;
            self.retained_output
                .admit_btree_entry::<String, XbrlOccurrenceRelationships>(
                    fact.occurrence_id
                        .len()
                        .checked_add(graph_clone_bytes)
                        .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
                )?;
            self.retained_output
                .admit_btree_entry::<String, usize>(fact.occurrence_id.len())?;
            occurrence_graph.insert(
                fact.occurrence_id.clone(),
                XbrlOccurrenceRelationships::try_new(
                    fact.parent_occurrence_id
                        .as_deref()
                        .map(SourceIdentifier::try_from)
                        .transpose()?,
                    child_occurrences,
                    fact.continuation_chain
                        .iter()
                        .map(|id| SourceIdentifier::try_from(id.as_str()))
                        .collect::<Result<Vec<_>, _>>()?,
                    incident,
                )?,
            );
            occurrence_graph_clone_bytes.insert(fact.occurrence_id.clone(), graph_clone_bytes);
        }
        let mut normalized = Vec::new();
        for fact in self.facts {
            check_xbrl_cancelled(cancellation)?;
            let context = self
                .contexts
                .get(&fact.context_id)
                .ok_or(SecXbrlError::UnknownContext)?;
            let unit_dynamic = fact
                .unit_id
                .as_ref()
                .and_then(|unit_id| self.units.get(unit_id))
                .map(unit_dynamic_bytes)
                .transpose()?
                .unwrap_or(0);
            let occurrence_dynamic = *occurrence_graph_clone_bytes
                .get(&fact.occurrence_id)
                .ok_or(SecXbrlError::ParserInvariant)?;
            let normalized_dynamic = checked_retained_sum([
                qname_dynamic_bytes(&fact.concept)?,
                fact.occurrence_id.len(),
                fact.context_id.len(),
                fact.unit_id.as_ref().map_or(0, String::capacity),
                fact.text
                    .trim()
                    .len()
                    .checked_mul(2)
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
                context.clone_dynamic_bytes()?,
                unit_dynamic,
                occurrence_dynamic,
                self.document.accession.retained_bytes(),
                SourceIdentifier::MAX_LENGTH,
                SourceIdentifier::MAX_LENGTH,
                self.document
                    .source_payload
                    .dynamic_retained_bytes()
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
                "sec-xbrl-duplicate-v2".len(),
                "sec-xbrl-parser-v2".len(),
                "sec-xbrl-rounding-v2".len(),
            ])?;
            self.retained_output.admit_vec_entry::<NormalizedDraft>(
                size_of::<XbrlFactEvidenceInput>()
                    .checked_add(normalized_dynamic)
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
            )?;
            normalized.push(NormalizedDraft::try_new(
                fact,
                &self.contexts,
                &self.units,
                &occurrence_graph,
                &self.document,
            )?);
        }
        let mut groups = BTreeMap::<[u8; 32], Vec<usize>>::new();
        for (index, draft) in normalized.iter().enumerate() {
            check_xbrl_cancelled(cancellation)?;
            if let NormalizedDraft::Numeric { evidence_input, .. } = draft {
                let digest = semantic_aspect_digest(evidence_input);
                if !groups.contains_key(&digest) {
                    self.retained_output
                        .admit_btree_entry::<[u8; 32], Vec<usize>>(0)?;
                }
                self.retained_output.admit_vec_entry::<usize>(0)?;
                groups.entry(digest).or_default().push(index);
            }
        }
        let mut group_metadata = BTreeMap::new();
        for (digest, indices) in groups {
            check_xbrl_cancelled(cancellation)?;
            let classification =
                classify_duplicate_group(&normalized, &indices, &mut self.retained_output)?;
            let group_id = if indices.len() == 1 {
                None
            } else {
                self.retained_output.admit("xbrl-duplicate-".len() + 32)?;
                Some(SourceIdentifier::try_from(format!(
                    "xbrl-duplicate-{}",
                    hex_prefix(&digest, 16)
                ))?)
            };
            self.retained_output
                .admit_btree_entry::<[u8; 32], (XbrlDuplicateClass, Option<SourceIdentifier>)>(
                    group_id
                        .as_ref()
                        .map_or(0, SourceIdentifier::retained_bytes),
                )?;
            group_metadata.insert(digest, (classification, group_id));
        }
        let mut numeric_facts = Vec::new();
        let mut nonnumeric_occurrences = Vec::new();
        for draft in normalized {
            check_xbrl_cancelled(cancellation)?;
            match draft {
                NormalizedDraft::Nonnumeric(value) => {
                    self.retained_output
                        .admit_vec_entry::<XbrlNonnumericOccurrence>(0)?;
                    nonnumeric_occurrences.push(*value);
                }
                NormalizedDraft::Numeric {
                    concept,
                    unit,
                    value,
                    evidence_input,
                    ..
                } => {
                    let digest = semantic_aspect_digest(&evidence_input);
                    let group_id_dynamic = group_metadata
                        .get(&digest)
                        .and_then(|(_, group_id)| group_id.as_ref())
                        .map_or(0, SourceIdentifier::retained_bytes);
                    self.retained_output.admit(group_id_dynamic)?;
                    self.retained_output.admit("sec-xbrl-duplicate-v2".len())?;
                    let (classification, group_id) = group_metadata
                        .get(&digest)
                        .cloned()
                        .ok_or(SecXbrlError::ParserInvariant)?;
                    let evidence = XbrlFactEvidence::try_new(XbrlFactEvidenceInput {
                        duplicate: XbrlDuplicateEvidence::try_new(
                            classification,
                            group_id,
                            SourceIdentifier::try_from("sec-xbrl-duplicate-v2")?,
                        )?,
                        ..*evidence_input
                    })?;
                    evidence.validate_value(value)?;
                    self.retained_output.admit_vec_entry::<XbrlNumericFact>(0)?;
                    numeric_facts.push(XbrlNumericFact {
                        concept,
                        unit,
                        value,
                        evidence,
                    });
                }
            }
        }
        Ok(ParsedXbrlDocument {
            accession: self.document.accession,
            expected_cik: self.document.expected_cik,
            taxonomy_set: self.document.taxonomy_set,
            source_payload: self.document.source_payload,
            evaluated_at: self.document.evaluated_at,
            retained_output_upper_bound: self.retained_output.admitted(),
            numeric_facts,
            nonnumeric_occurrences,
        })
    }
}

fn semantic_aspect_digest(input: &XbrlFactEvidenceInput) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hash_expanded_name(&mut hasher, &input.concept);
    hash_text(&mut hasher, input.entity.scheme().as_str());
    hash_text(&mut hasher, input.entity.value().as_str());
    hash_period(&mut hasher, input.period);
    hash_unit(&mut hasher, &input.unit);
    let mut dimensions = input
        .dimensions
        .iter()
        .map(dimension_digest)
        .collect::<Vec<_>>();
    dimensions.sort_unstable();
    hasher.update((dimensions.len() as u64).to_be_bytes());
    for dimension in dimensions {
        hasher.update(dimension);
    }
    hash_non_dimensional_context_content(&mut hasher, &input.context_graph);
    hasher.finalize().into()
}

fn hash_non_dimensional_context_content(hasher: &mut Sha256, graph: &XbrlContextGraph) {
    let mut content_hasher = Sha256::new();
    let mut event_count = 0u64;
    let mut non_dimensional_depth = 0usize;
    let mut skipped_dimension_depth = 0usize;
    for event in graph.events() {
        match event {
            XbrlXmlEvent::Start { name } if skipped_dimension_depth > 0 => {
                skipped_dimension_depth += 1;
            }
            XbrlXmlEvent::Start { name }
                if is_element(name, XBRLDI_NAMESPACE, "explicitMember")
                    || is_element(name, XBRLDI_NAMESPACE, "typedMember") =>
            {
                skipped_dimension_depth = 1;
            }
            XbrlXmlEvent::Start { name }
                if is_element(name, XBRLI_NAMESPACE, "segment")
                    || is_element(name, XBRLI_NAMESPACE, "scenario") => {}
            XbrlXmlEvent::Start { name } => {
                event_count += 1;
                non_dimensional_depth += 1;
                content_hasher.update([0]);
                hash_expanded_name(&mut content_hasher, name);
            }
            XbrlXmlEvent::Attribute { .. } if skipped_dimension_depth > 0 => {}
            XbrlXmlEvent::Attribute { name, value } => {
                event_count += 1;
                content_hasher.update([1]);
                hash_expanded_name(&mut content_hasher, name);
                hash_text(&mut content_hasher, value.as_str());
            }
            XbrlXmlEvent::Text { .. } if skipped_dimension_depth > 0 => {}
            XbrlXmlEvent::Text { value }
                if non_dimensional_depth > 0 || !value.as_str().trim().is_empty() =>
            {
                event_count += 1;
                content_hasher.update([2]);
                hash_text(&mut content_hasher, value.as_str());
            }
            XbrlXmlEvent::Text { .. } => {}
            XbrlXmlEvent::End { .. } if skipped_dimension_depth > 0 => {
                skipped_dimension_depth -= 1;
            }
            XbrlXmlEvent::End { name }
                if is_element(name, XBRLI_NAMESPACE, "segment")
                    || is_element(name, XBRLI_NAMESPACE, "scenario") => {}
            XbrlXmlEvent::End { name } => {
                event_count += 1;
                non_dimensional_depth -= 1;
                content_hasher.update([3]);
                hash_expanded_name(&mut content_hasher, name);
            }
        }
    }
    hasher.update(event_count.to_be_bytes());
    if event_count > 0 {
        hasher.update(content_hasher.finalize());
    }
}

fn dimension_digest(dimension: &XbrlDimensionEvidence) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hash_expanded_name(&mut hasher, dimension.dimension());
    match dimension.member() {
        XbrlDimensionMember::Explicit { member } => {
            hasher.update([0]);
            hash_expanded_name(&mut hasher, member);
        }
        XbrlDimensionMember::Typed { source_graph, .. } => {
            hasher.update([1]);
            hash_source_graph(&mut hasher, source_graph);
        }
    }
    hasher.finalize().into()
}

fn hash_source_graph(hasher: &mut Sha256, graph: &XbrlContextGraph) {
    hasher.update((graph.events().len() as u64).to_be_bytes());
    for event in graph.events() {
        match event {
            XbrlXmlEvent::Start { name } => {
                hasher.update([0]);
                hash_expanded_name(hasher, name);
            }
            XbrlXmlEvent::Attribute { name, value } => {
                hasher.update([1]);
                hash_expanded_name(hasher, name);
                hash_text(hasher, value.as_str());
            }
            XbrlXmlEvent::Text { value } => {
                hasher.update([2]);
                hash_text(hasher, value.as_str());
            }
            XbrlXmlEvent::End { name } => {
                hasher.update([3]);
                hash_expanded_name(hasher, name);
            }
        }
    }
}

fn hash_unit(hasher: &mut Sha256, unit: &XbrlUnitExpression) {
    if let Some(measure) = unit.measure_name() {
        hasher.update([0]);
        hash_expanded_name(hasher, measure);
    } else if let Some((numerator, denominator)) = unit.divide_parts() {
        hasher.update([1]);
        hash_name_multiset(hasher, numerator);
        hash_name_multiset(hasher, denominator);
    }
}

fn hash_name_multiset(hasher: &mut Sha256, names: &[XbrlQualifiedName]) {
    let mut digests = names
        .iter()
        .map(|name| {
            let mut name_hasher = Sha256::new();
            hash_expanded_name(&mut name_hasher, name);
            <[u8; 32]>::from(name_hasher.finalize())
        })
        .collect::<Vec<_>>();
    digests.sort_unstable();
    hasher.update((digests.len() as u64).to_be_bytes());
    for digest in digests {
        hasher.update(digest);
    }
}

fn hash_period(hasher: &mut Sha256, period: XbrlPeriod) {
    match period {
        XbrlPeriod::Instant { instant } => {
            hasher.update([0]);
            hash_text(hasher, &instant.to_string());
        }
        XbrlPeriod::Duration { start, end } => {
            hasher.update([1]);
            hash_text(hasher, &start.to_string());
            hash_text(hasher, &end.to_string());
        }
    }
}

fn hash_expanded_name(hasher: &mut Sha256, name: &XbrlQualifiedName) {
    match name.namespace_uri() {
        Some(namespace) => {
            hasher.update([1]);
            hash_text(hasher, namespace.as_str());
        }
        None => hasher.update([0]),
    }
    hash_text(hasher, name.local_name().as_str());
}

fn hash_text(hasher: &mut Sha256, text: &str) {
    hasher.update((text.len() as u64).to_be_bytes());
    hasher.update(text.as_bytes());
}

fn classify_duplicate_group(
    drafts: &[NormalizedDraft],
    indices: &[usize],
    retained_output: &mut RetainedOutputBudget,
) -> Result<XbrlDuplicateClass, SecXbrlError> {
    if indices.len() == 1 {
        return Ok(XbrlDuplicateClass::Unique);
    }
    let mut values_at_accuracy = BTreeMap::<EffectiveAccuracy, Decimal>::new();
    let mut common_interval: Option<(Decimal, Decimal)> = None;
    for index in indices {
        let NormalizedDraft::Numeric {
            value,
            evidence_input,
            ..
        } = drafts.get(*index).ok_or(SecXbrlError::ParserInvariant)?
        else {
            return Err(SecXbrlError::ParserInvariant);
        };
        let accuracy_key = effective_accuracy(*value, evidence_input.accuracy);
        if !values_at_accuracy.contains_key(&accuracy_key) {
            retained_output.admit_btree_entry::<EffectiveAccuracy, Decimal>(0)?;
        }
        if values_at_accuracy
            .insert(accuracy_key, *value)
            .is_some_and(|existing| existing != *value)
        {
            return Ok(XbrlDuplicateClass::Inconsistent);
        }
        let Some((lower, upper)) = accuracy_interval(*value, evidence_input.accuracy) else {
            return Ok(XbrlDuplicateClass::Unclassified);
        };
        common_interval = Some(match common_interval {
            None => (lower, upper),
            Some((current_lower, current_upper)) => {
                let lower = if lower > current_lower {
                    lower
                } else {
                    current_lower
                };
                let upper = if upper < current_upper {
                    upper
                } else {
                    current_upper
                };
                (lower, upper)
            }
        });
    }
    Ok(
        if common_interval.is_some_and(|(lower, upper)| lower <= upper) {
            XbrlDuplicateClass::ConsistentNumeric
        } else {
            XbrlDuplicateClass::Inconsistent
        },
    )
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum EffectiveAccuracy {
    Exact,
    Decimals(i32),
    Unspecified,
}

fn effective_accuracy(value: Decimal, accuracy: XbrlAccuracy) -> EffectiveAccuracy {
    match accuracy {
        XbrlAccuracy::Decimals(XbrlAccuracyValue::Finite(decimals)) => {
            EffectiveAccuracy::Decimals(decimals)
        }
        XbrlAccuracy::Decimals(XbrlAccuracyValue::Infinite)
        | XbrlAccuracy::Precision(XbrlAccuracyValue::Infinite) => EffectiveAccuracy::Exact,
        XbrlAccuracy::Precision(XbrlAccuracyValue::Finite(precision)) => {
            effective_decimals(value, precision)
                .map_or(EffectiveAccuracy::Unspecified, EffectiveAccuracy::Decimals)
        }
        XbrlAccuracy::Unspecified => EffectiveAccuracy::Unspecified,
    }
}

fn accuracy_interval(value: Decimal, accuracy: XbrlAccuracy) -> Option<(Decimal, Decimal)> {
    let decimals = match accuracy {
        XbrlAccuracy::Decimals(XbrlAccuracyValue::Infinite)
        | XbrlAccuracy::Precision(XbrlAccuracyValue::Infinite) => return Some((value, value)),
        XbrlAccuracy::Decimals(XbrlAccuracyValue::Finite(decimals)) => decimals,
        XbrlAccuracy::Precision(XbrlAccuracyValue::Finite(precision)) => {
            effective_decimals(value, precision)?
        }
        XbrlAccuracy::Unspecified => return None,
    };
    let radius = half_unit_in_last_place(decimals)?;
    Some((value.checked_sub(radius)?, value.checked_add(radius)?))
}

fn effective_decimals(value: Decimal, precision: i32) -> Option<i32> {
    if precision <= 0 || value.is_zero() {
        return None;
    }
    let normalized = value.normalize();
    let digits = i32::try_from(normalized.mantissa().unsigned_abs().ilog10() + 1).ok()?;
    let scale = i32::try_from(normalized.scale()).ok()?;
    let magnitude = digits.checked_sub(scale)?.checked_sub(1)?;
    precision.checked_sub(magnitude)?.checked_sub(1)
}

fn half_unit_in_last_place(decimals: i32) -> Option<Decimal> {
    let exponent = decimals.checked_neg()?.checked_sub(1)?;
    if exponent >= 0 {
        let mut value = Decimal::from(5);
        for _ in 0..u32::try_from(exponent).ok()? {
            value = value.checked_mul(Decimal::TEN)?;
        }
        Some(value)
    } else {
        let scale = u32::try_from(exponent.checked_neg()?).ok()?;
        (scale <= Decimal::MAX_SCALE).then(|| Decimal::new(5, scale))
    }
}

struct ContextDraft {
    id: String,
    entity_scheme: Option<String>,
    entity_value: Option<String>,
    instant: Option<CalendarDate>,
    start: Option<CalendarDate>,
    end: Option<CalendarDate>,
    dimensions: Vec<XbrlDimensionEvidence>,
    graph_events: Vec<XbrlXmlEvent>,
}

impl ContextDraft {
    fn new(id: String) -> Self {
        Self {
            id,
            entity_scheme: None,
            entity_value: None,
            instant: None,
            start: None,
            end: None,
            dimensions: Vec::new(),
            graph_events: Vec::new(),
        }
    }

    fn period(&self) -> Result<XbrlPeriod, SecXbrlError> {
        match (self.instant, self.start, self.end) {
            (Some(instant), None, None) => Ok(XbrlPeriod::instant(instant)),
            (None, Some(start), Some(end)) => Ok(XbrlPeriod::duration(start, end)?),
            _ => Err(SecXbrlError::IncompleteContext),
        }
    }

    fn clone_dynamic_bytes(&self) -> Result<usize, SecXbrlError> {
        let dimensions = self.dimensions.iter().try_fold(
            self.dimensions
                .len()
                .checked_mul(size_of::<XbrlDimensionEvidence>())
                .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
            |total, dimension| {
                total
                    .checked_add(dimension_dynamic_bytes(dimension)?)
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
            },
        )?;
        let graph = self.graph_events.iter().try_fold(
            self.graph_events
                .len()
                .checked_mul(size_of::<XbrlXmlEvent>())
                .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
            |total, event| {
                total
                    .checked_add(xml_event_dynamic_bytes(event)?)
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
            },
        )?;
        checked_retained_sum([
            self.entity_scheme.as_ref().map_or(0, String::capacity),
            self.entity_value.as_ref().map_or(0, String::capacity),
            dimensions,
            graph,
        ])
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum UnitSide {
    Numerator,
    Denominator,
}

struct UnitDraft {
    id: String,
    divide: bool,
    divide_open: bool,
    side: Option<UnitSide>,
    simple: Vec<XbrlQualifiedName>,
    numerator: Vec<XbrlQualifiedName>,
    denominator: Vec<XbrlQualifiedName>,
}

impl UnitDraft {
    fn new(id: String) -> Self {
        Self {
            id,
            divide: false,
            divide_open: false,
            side: None,
            simple: Vec::new(),
            numerator: Vec::new(),
            denominator: Vec::new(),
        }
    }

    fn start_divide(&mut self) -> Result<(), SecXbrlError> {
        if self.divide || !self.simple.is_empty() || self.side.is_some() {
            return Err(SecXbrlError::InvalidUnitExpression);
        }
        self.divide = true;
        self.divide_open = true;
        Ok(())
    }

    fn end_divide(&mut self) -> Result<(), SecXbrlError> {
        if !self.divide_open || self.side.is_some() {
            return Err(SecXbrlError::InvalidUnitExpression);
        }
        self.divide_open = false;
        Ok(())
    }

    fn start_side(&mut self, side: UnitSide) -> Result<(), SecXbrlError> {
        if !self.divide_open || self.side.is_some() {
            return Err(SecXbrlError::InvalidUnitExpression);
        }
        self.side = Some(side);
        Ok(())
    }

    fn end_side(&mut self, side: UnitSide) -> Result<(), SecXbrlError> {
        if self.side != Some(side) {
            return Err(SecXbrlError::InvalidUnitExpression);
        }
        self.side = None;
        Ok(())
    }

    fn push_measure(&mut self, measure: XbrlQualifiedName) -> Result<(), SecXbrlError> {
        match (self.divide, self.side) {
            (false, None) => self.simple.push(measure),
            (true, Some(UnitSide::Numerator)) => self.numerator.push(measure),
            (true, Some(UnitSide::Denominator)) => self.denominator.push(measure),
            _ => return Err(SecXbrlError::InvalidUnitExpression),
        }
        Ok(())
    }

    fn finish(self) -> Result<XbrlUnitExpression, SecXbrlError> {
        if self.divide {
            if self.divide_open || self.side.is_some() || !self.simple.is_empty() {
                return Err(SecXbrlError::InvalidUnitExpression);
            }
            XbrlUnitExpression::divide(self.numerator, self.denominator)
                .map_err(|_| SecXbrlError::InvalidUnitExpression)
        } else {
            if self.simple.len() != 1 {
                return Err(SecXbrlError::IncompleteUnit);
            }
            let measure = self
                .simple
                .into_iter()
                .next()
                .ok_or(SecXbrlError::IncompleteUnit)?;
            Ok(XbrlUnitExpression::measure(measure))
        }
    }
}

struct Capture {
    depth: usize,
    kind: CaptureKind,
    text: String,
}

enum CaptureKind {
    Identifier {
        scheme: String,
    },
    Instant,
    StartDate,
    EndDate,
    ExplicitMember {
        dimension: XbrlQualifiedName,
        location: XbrlDimensionLocation,
    },
    TypedMember {
        dimension: XbrlQualifiedName,
        location: XbrlDimensionLocation,
        graph_start: usize,
    },
    Measure,
}

impl CaptureKind {
    fn dynamic_bytes(&self) -> Result<usize, SecXbrlError> {
        match self {
            Self::Identifier { scheme } => Ok(scheme.len()),
            Self::ExplicitMember { dimension, .. } | Self::TypedMember { dimension, .. } => {
                qname_dynamic_bytes(dimension)
            }
            Self::Instant | Self::StartDate | Self::EndDate | Self::Measure => Ok(0),
        }
    }
}

struct FactDraft {
    start_depth: usize,
    concept: XbrlQualifiedName,
    context_id: String,
    unit_id: Option<String>,
    occurrence_id: String,
    parent_occurrence_id: Option<String>,
    accuracy: XbrlAccuracy,
    scale: Option<i32>,
    sign: Option<XbrlSign>,
    format: Option<XbrlQualifiedName>,
    language: Option<String>,
    nil: bool,
    explicitly_nonnumeric: bool,
    continued_at: Option<String>,
    continuation_chain: Vec<String>,
    text: String,
}

struct ContinuationDraft {
    start_depth: usize,
    id: String,
    continued_at: Option<String>,
    text: String,
}

struct RelationshipDraft {
    arcrole: String,
    from_refs: Vec<String>,
    to_refs: Vec<String>,
    link_role: Option<String>,
    order: Option<String>,
}

impl RelationshipDraft {
    fn dynamic_bytes_from_attributes(
        attributes: &ResolvedAttributes,
    ) -> Result<usize, SecXbrlError> {
        let arcrole = attributes
            .unqualified("arcrole")
            .ok_or(SecXbrlError::MissingAttribute)?;
        let from_refs = attributes
            .unqualified("fromRefs")
            .ok_or(SecXbrlError::MissingAttribute)?;
        let to_refs = attributes
            .unqualified("toRefs")
            .ok_or(SecXbrlError::MissingAttribute)?;
        let reference_storage = from_refs
            .split_whitespace()
            .chain(to_refs.split_whitespace())
            .try_fold(0usize, |total, reference| {
                total
                    .checked_add(size_of::<String>())
                    .and_then(|bytes| bytes.checked_add(reference.len()))
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
            })?;
        checked_retained_sum([
            arcrole.len(),
            reference_storage,
            attributes.unqualified("linkRole").map_or(0, str::len),
            attributes.unqualified("order").map_or(0, str::len),
        ])
    }

    fn evidence_dynamic_bytes(&self) -> Result<usize, SecXbrlError> {
        let reference_storage = self.from_refs.iter().chain(&self.to_refs).try_fold(
            self.from_refs
                .len()
                .checked_add(self.to_refs.len())
                .and_then(|length| length.checked_mul(size_of::<SourceIdentifier>()))
                .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?,
            |total, reference| {
                total
                    .checked_add(reference.capacity())
                    .ok_or(SecXbrlError::RetainedOutputLimitExceeded)
            },
        )?;
        checked_retained_sum([
            self.arcrole.capacity(),
            reference_storage,
            self.link_role.as_ref().map_or(0, String::capacity),
            self.order.as_ref().map_or(0, String::capacity),
        ])
    }

    fn try_new(attributes: &ResolvedAttributes) -> Result<Self, SecXbrlError> {
        Ok(Self {
            arcrole: attributes.required_unqualified("arcrole")?,
            from_refs: split_references(&attributes.required_unqualified("fromRefs")?),
            to_refs: split_references(&attributes.required_unqualified("toRefs")?),
            link_role: attributes.unqualified("linkRole").map(str::to_owned),
            order: attributes.unqualified("order").map(str::to_owned),
        })
    }

    fn into_evidence(self) -> Result<XbrlRelationshipEvidence, SecXbrlError> {
        Ok(XbrlRelationshipEvidence::try_new(
            SourceIdentifier::try_from(self.arcrole)?,
            self.from_refs
                .into_iter()
                .map(SourceIdentifier::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            self.to_refs
                .into_iter()
                .map(SourceIdentifier::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            self.link_role.map(SourceIdentifier::try_from).transpose()?,
            self.order.map(XbrlText::try_from).transpose()?,
        )?)
    }
}

fn split_references(value: &str) -> Vec<String> {
    value.split_whitespace().map(str::to_owned).collect()
}
