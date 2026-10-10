//! Bounded namespace-authoritative parser for XBRL and Inline XBRL occurrences.

mod acquisition;
mod model;
mod normalize;
mod number_words;
mod staging;
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

pub(crate) use acquisition::{SecTaxonomyAcquisitionRequest, SecTaxonomyClosure};
pub(crate) use model::{
    MAX_TAXONOMY_ARTIFACT_BYTES, SecPendingValidatedXbrlTaxonomySet, SecValidatedXbrlTaxonomySet,
    SecXbrlTaxonomyArtifact, SecXbrlTaxonomyReference, SecXbrlTaxonomyRegistry,
};
pub use model::{
    ParsedXbrlDocument, XbrlDocumentContext, XbrlFootnoteOccurrence, XbrlNonnumericOccurrence,
    XbrlNumericFact,
};
use normalize::NormalizedDraft;
pub(crate) use staging::IndexedXbrlDocument;
pub use support::SecXbrlError;
use wire::*;

/// Prospective event allocations include collection-growth headroom. At each event boundary,
/// charges are rebased to the live open XML state: completed records now belong to disk staging.
/// Cumulative work and durable filing size are not retained-memory charges.
const RETAINED_CAPACITY_ALLOWANCE: usize = 2;
const BTREE_LINK_WORDS_PER_ENTRY: usize = 4;

#[derive(Debug)]
struct RetainedOutputBudget {
    admitted: usize,
    peak: usize,
    limit: usize,
}

impl RetainedOutputBudget {
    const fn new(limit: usize) -> Self {
        Self {
            admitted: 0,
            peak: 0,
            limit,
        }
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
        self.peak = self.peak.max(admitted);
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
        Self::parse_indexed_with_cancellation(bytes, limits, document, cancellation)?
            .materialize(cancellation)
    }

    /// Parses and validates a complete filing using disk-backed indexes. Finalized source
    /// occurrences remain in the operation-owned index until individually consumed.
    pub(crate) fn parse_indexed_with_cancellation(
        bytes: &[u8],
        limits: SecParserLimits,
        document: XbrlDocumentContext,
        cancellation: &CancellationToken,
    ) -> Result<IndexedXbrlDocument, SecXbrlError> {
        let result = Self::parse_indexed(bytes, limits, document, cancellation, None);
        check_xbrl_cancelled(cancellation)?;
        result
    }

    /// Uses an application-owned operation directory so crash recovery can reclaim scratch.
    pub(crate) fn parse_indexed_in_with_cancellation(
        bytes: &[u8],
        limits: SecParserLimits,
        document: XbrlDocumentContext,
        cancellation: &CancellationToken,
        scratch_parent: &std::path::Path,
    ) -> Result<IndexedXbrlDocument, SecXbrlError> {
        let result =
            Self::parse_indexed(bytes, limits, document, cancellation, Some(scratch_parent));
        check_xbrl_cancelled(cancellation)?;
        result
    }

    fn parse_indexed(
        bytes: &[u8],
        limits: SecParserLimits,
        document: XbrlDocumentContext,
        cancellation: &CancellationToken,
        scratch_parent: Option<&std::path::Path>,
    ) -> Result<IndexedXbrlDocument, SecXbrlError> {
        if bytes.len() > limits.decoded_bytes() {
            return Err(SecXbrlError::ByteLimitExceeded);
        }
        check_xbrl_cancelled(cancellation)?;
        let scratch = parser_scratch_reservation(bytes, limits, cancellation)?;
        let output_admission = limits
            .retained_output_bytes()
            .checked_sub(scratch)
            .filter(|remaining| *remaining > 0)
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
        let limits = limits
            .with_retained_bytes(output_admission)
            .map_err(|_| SecXbrlError::RetainedOutputLimitExceeded)?;
        let mut reader = NsReader::from_reader(bytes);
        reader.config_mut().trim_text(false);
        reader.config_mut().expand_empty_elements = true;
        let mut state = ParserState::new(limits, document, cancellation, scratch_parent)?;
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
            state.refresh_retained()?;
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
    index: staging::FilingIndex,
    depth: usize,
    current_context: Option<ContextDraft>,
    current_unit: Option<UnitDraft>,
    context_container: Option<(usize, XbrlDimensionLocation)>,
    capture: Option<Capture>,
    active_facts: Vec<FactDraft>,
    active_footnotes: Vec<FootnoteDraft>,
    language_scopes: Vec<(usize, String)>,
    active_continuations: Vec<ContinuationDraft>,
    element_ordinal: usize,
    exclude_depths: Vec<usize>,
    next_fact_ordinal: usize,
}

impl ParserState {
    fn new(
        limits: SecParserLimits,
        document: XbrlDocumentContext,
        cancellation: &CancellationToken,
        scratch_parent: Option<&std::path::Path>,
    ) -> Result<Self, SecXbrlError> {
        Ok(Self {
            index: staging::FilingIndex::new(
                limits.retained_output_bytes(),
                cancellation,
                scratch_parent,
            )?,
            retained_output: RetainedOutputBudget::new(limits.retained_output_bytes()),
            limits,
            document,
            depth: 0,
            current_context: None,
            current_unit: None,
            context_container: None,
            capture: None,
            active_facts: Vec::new(),
            active_footnotes: Vec::new(),
            language_scopes: Vec::new(),
            active_continuations: Vec::new(),
            element_ordinal: 0,
            exclude_depths: Vec::new(),
            next_fact_ordinal: 0,
        })
    }

    fn refresh_retained(&mut self) -> Result<(), SecXbrlError> {
        let mut live = 2 * 1024 * 1024 + size_of::<Self>(); // SQLite page cache, not corpus size.
        live = checked_retained_sum([
            live,
            self.active_facts.capacity() * size_of::<FactDraft>(),
            self.active_footnotes.capacity() * size_of::<FootnoteDraft>(),
            self.active_continuations.capacity() * size_of::<ContinuationDraft>(),
            self.exclude_depths.capacity() * size_of::<usize>(),
            self.language_scopes.capacity() * size_of::<(usize, String)>(),
        ])?;
        for (_, language) in &self.language_scopes {
            live = checked_retained_sum([live, language.capacity()])?;
        }
        for fact in &self.active_facts {
            live = checked_retained_sum([live, fact.dynamic_bytes()?])?;
        }
        for note in &self.active_footnotes {
            live = checked_retained_sum([
                live,
                note.id.capacity(),
                note.language.capacity(),
                note.role.capacity(),
                note.title.as_ref().map_or(0, String::capacity),
                note.continued_at.as_ref().map_or(0, String::capacity),
                note.text.capacity(),
            ])?;
        }
        for continuation in &self.active_continuations {
            live = checked_retained_sum([
                live,
                continuation.id.capacity(),
                continuation
                    .continued_at
                    .as_ref()
                    .map_or(0, String::capacity),
                continuation.text.capacity(),
            ])?;
        }
        if let Some(context) = &self.current_context {
            live = checked_retained_sum([
                live,
                context.id.capacity(),
                context.clone_dynamic_bytes()?,
            ])?;
        }
        if let Some(unit) = &self.current_unit {
            live = checked_retained_sum([
                live,
                unit.id.capacity(),
                (unit.simple.capacity() + unit.numerator.capacity() + unit.denominator.capacity())
                    * size_of::<XbrlQualifiedName>(),
            ])?;
            for name in unit
                .simple
                .iter()
                .chain(&unit.numerator)
                .chain(&unit.denominator)
            {
                live = checked_retained_sum([live, qname_dynamic_bytes(name)?])?;
            }
        }
        if let Some(capture) = &self.capture {
            live = checked_retained_sum([
                live,
                capture.text.capacity(),
                capture.kind.dynamic_bytes()?,
            ])?;
        }
        self.retained_output.admitted = 0;
        self.retained_output.admit(live)
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

        if let Some(language) =
            attributes.namespaced("http://www.w3.org/XML/1998/namespace", "lang")
        {
            self.retained_output
                .admit_vec_entry::<(usize, String)>(language.len())?;
            self.language_scopes.push((self.depth, language.to_owned()));
        }
        self.element_ordinal = self
            .element_ordinal
            .checked_add(1)
            .ok_or(SecXbrlError::RecordLimitExceeded)?;
        if name
            .namespace_uri()
            .is_some_and(|namespace| namespace.as_str() == IX_NAMESPACE)
        {
            if let Some(reference) = attributes.unqualified("continuedAt") {
                self.retained_output
                    .admit_btree_entry::<String, ()>(reference.len())?;
                self.index.reference(reference)?;
            }
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

        if is_element(&name, IX_NAMESPACE, "footnote") {
            if self
                .index
                .count("footnote")?
                .checked_add(self.active_footnotes.len())
                .is_none_or(|count| count >= self.limits.records())
            {
                return Err(SecXbrlError::RecordLimitExceeded);
            }
            let id = attributes
                .unqualified("id")
                .ok_or(SecXbrlError::MissingAttribute)?;
            let language = self
                .language_scopes
                .last()
                .map(|(_, language)| language.as_str())
                .filter(|language| !language.is_empty())
                .ok_or(SecXbrlError::MissingAttribute)?;
            let role = attributes
                .unqualified("footnoteRole")
                .unwrap_or("http://www.xbrl.org/2003/role/footnote");
            let title = attributes.unqualified("title");
            let continued_at = attributes.unqualified("continuedAt");
            self.retained_output
                .admit_vec_entry::<FootnoteDraft>(checked_retained_sum([
                    id.len(),
                    language.len(),
                    role.len(),
                    title.map_or(0, str::len),
                    continued_at.map_or(0, str::len),
                ])?)?;
            self.active_footnotes.push(FootnoteDraft {
                start_depth: self.depth,
                span: ElementSpan::new(self.element_ordinal),
                id: id.to_owned(),
                language: language.to_owned(),
                role: role.to_owned(),
                title: title.map(str::to_owned),
                continued_at: continued_at.map(str::to_owned),
                continuation_chain: Vec::new(),
                text: String::new(),
            });
            return Ok(());
        }
        if is_element(&name, IX_NAMESPACE, "continuation") {
            let id = attributes
                .unqualified("id")
                .ok_or(SecXbrlError::MissingAttribute)?;
            let continued_at = attributes.unqualified("continuedAt");
            if self
                .index
                .count("continuation")?
                .checked_add(self.active_continuations.len())
                .is_none_or(|count| count >= self.limits.records())
            {
                return Err(SecXbrlError::RecordLimitExceeded);
            }
            self.retained_output
                .admit_vec_entry::<ContinuationDraft>(checked_retained_sum([
                    id.len(),
                    continued_at.map_or(0, str::len),
                ])?)?;
            self.active_continuations.push(ContinuationDraft {
                start_depth: self.depth,
                span: ElementSpan::new(self.element_ordinal),
                id: id.to_owned(),
                continued_at: continued_at.map(str::to_owned),
                text: String::new(),
            });
            return Ok(());
        }
        if is_element(&name, IX_NAMESPACE, "exclude") {
            self.retained_output.admit_vec_entry::<usize>(0)?;
            self.exclude_depths.push(self.depth);
            return Ok(());
        }
        if is_element(&name, IX_NAMESPACE, "relationship") {
            self.retained_output.admit_vec_entry::<RelationshipDraft>(
                RelationshipDraft::dynamic_bytes_from_attributes(&attributes)?,
            )?;
            self.index
                .relationship(RelationshipDraft::try_new(&attributes)?)?;
            if self.index.count("relationship")? > self.limits.records() {
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
                span: ElementSpan::new(self.element_ordinal),
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
        // An exclusion suppresses its ancestor capture, but independently tagged
        // descendants inside that exclusion still retain their own source content.
        let excluded_at = self.exclude_depths.last().copied();
        let included = |start_depth| excluded_at.is_none_or(|depth| depth < start_depth);
        let copies = self
            .active_facts
            .iter()
            .filter(|fact| included(fact.start_depth))
            .count()
            .checked_add(
                self.active_continuations
                    .iter()
                    .filter(|continuation| included(continuation.start_depth))
                    .count(),
            )
            .and_then(|copies| {
                copies.checked_add(
                    self.active_footnotes
                        .iter()
                        .filter(|footnote| included(footnote.start_depth))
                        .count(),
                )
            })
            .and_then(|copies| copies.checked_mul(text.len()))
            .ok_or(SecXbrlError::RetainedOutputLimitExceeded)?;
        self.retained_output.admit(copies)?;
        for fact in &mut self.active_facts {
            if included(fact.start_depth) {
                append_bounded(&mut fact.text, text, self.limits.string_bytes())?;
            }
        }
        for continuation in &mut self.active_continuations {
            if included(continuation.start_depth) {
                append_bounded(&mut continuation.text, text, self.limits.string_bytes())?;
            }
        }
        for footnote in &mut self.active_footnotes {
            if included(footnote.start_depth) {
                append_bounded(&mut footnote.text, text, self.limits.string_bytes())?;
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
            .active_footnotes
            .last()
            .is_some_and(|footnote| footnote.start_depth == self.depth)
        {
            let mut footnote = self
                .active_footnotes
                .pop()
                .ok_or(SecXbrlError::ParserInvariant)?;
            footnote.span.end = self.element_ordinal;
            self.retained_output.admit_vec_entry::<FootnoteDraft>(0)?;
            self.index.insert(
                "footnote",
                &footnote.id,
                self.index.count("footnote")?,
                &footnote,
            )?;
        }
        if self
            .active_continuations
            .last()
            .is_some_and(|continuation| continuation.start_depth == self.depth)
        {
            let mut continuation = self
                .active_continuations
                .pop()
                .ok_or(SecXbrlError::ParserInvariant)?;
            continuation.span.end = self.element_ordinal;
            self.retained_output
                .admit_btree_entry::<String, ContinuationDraft>(continuation.id.len())?;
            self.index.insert(
                "continuation",
                &continuation.id,
                self.index.count("continuation")?,
                &continuation,
            )?;
        }
        if self
            .active_facts
            .last()
            .is_some_and(|fact| fact.start_depth == self.depth)
        {
            let mut fact = self
                .active_facts
                .pop()
                .ok_or(SecXbrlError::ParserInvariant)?;
            fact.span.end = self.element_ordinal;
            self.retained_output.admit_vec_entry::<FactDraft>(0)?;
            self.index.fact(&fact)?;
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
        if is_element(name, IX_NAMESPACE, "exclude")
            && self.exclude_depths.last() == Some(&self.depth)
        {
            self.exclude_depths.pop();
        }
        if is_element(name, XBRLI_NAMESPACE, "context") {
            let context = self
                .current_context
                .take()
                .ok_or(SecXbrlError::ParserInvariant)?;
            self.retained_output
                .admit_btree_entry::<String, ContextDraft>(context.id.len())?;
            self.index.insert(
                "context",
                &context.id,
                self.index.count("context")?,
                &context,
            )?;
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
            self.index
                .insert("unit", &id, self.index.count("unit")?, &expression)?;
        }
        if self
            .language_scopes
            .last()
            .is_some_and(|(depth, _)| *depth == self.depth)
        {
            self.language_scopes.pop();
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
    ) -> Result<IndexedXbrlDocument, SecXbrlError> {
        check_xbrl_cancelled(cancellation)?;
        if self.depth != 0
            || self.current_context.is_some()
            || self.current_unit.is_some()
            || !self.active_facts.is_empty()
            || !self.active_footnotes.is_empty()
            || !self.language_scopes.is_empty()
            || !self.active_continuations.is_empty()
            || self.capture.is_some()
            || !self.exclude_depths.is_empty()
            || self.context_container.is_some()
        {
            return Err(SecXbrlError::UnexpectedEof);
        }
        for ordinal in 0..self.index.count("footnote")? {
            check_xbrl_cancelled(cancellation)?;
            let footnote: FootnoteDraft = self
                .index
                .at("footnote", ordinal)?
                .ok_or(SecXbrlError::ParserInvariant)?;
            if self.index.contains("fact", &footnote.id)?
                || self.index.contains("continuation", &footnote.id)?
            {
                return Err(SecXbrlError::DuplicateIdentity);
            }
        }
        for ordinal in 0..self.index.count("relationship")? {
            check_xbrl_cancelled(cancellation)?;
            let relation: XbrlRelationshipEvidence = self
                .index
                .at("relationship", ordinal)?
                .ok_or(SecXbrlError::ParserInvariant)?;
            for id in relation.from_refs() {
                if !self.index.contains("fact", id.as_str())? {
                    return Err(SecXbrlError::UnknownRelationshipReference);
                }
            }
            let mut footnote_targets = 0;
            for id in relation.to_refs() {
                if self.index.contains("footnote", id.as_str())? {
                    footnote_targets += 1;
                } else if !self.index.contains("fact", id.as_str())? {
                    return Err(SecXbrlError::UnknownRelationshipReference);
                }
                if relation.from_refs().contains(id) {
                    return Err(SecXbrlError::InvalidRelationshipGraph);
                }
            }
            if footnote_targets != 0 && footnote_targets != relation.to_refs().len() {
                return Err(SecXbrlError::InvalidRelationshipGraph);
            }
        }
        for ordinal in 0..self.index.count("fact")? {
            check_xbrl_cancelled(cancellation)?;
            self.refresh_retained()?;
            let mut fact: FactDraft = self
                .index
                .at("fact", ordinal)?
                .ok_or(SecXbrlError::ParserInvariant)?;
            resolve_continuations(
                fact.span,
                &mut fact.continued_at,
                &mut fact.text,
                &mut fact.continuation_chain,
                &self.index,
                &mut self.retained_output,
                self.limits.string_bytes(),
                cancellation,
            )?;
            let context: ContextDraft = self
                .index
                .get("context", &fact.context_id)?
                .ok_or(SecXbrlError::UnknownContext)?;
            let unit = fact
                .unit_id
                .as_ref()
                .map(|id| self.index.get::<XbrlUnitExpression>("unit", id))
                .transpose()?
                .flatten();
            let graph = XbrlOccurrenceRelationships::try_new(
                fact.parent_occurrence_id
                    .as_deref()
                    .map(SourceIdentifier::try_from)
                    .transpose()?,
                self.index.children(&fact.occurrence_id)?,
                fact.continuation_chain
                    .iter()
                    .map(|id| SourceIdentifier::try_from(id.as_str()))
                    .collect::<Result<Vec<_>, _>>()?,
                self.index.incident(&fact.occurrence_id)?,
            )?;
            let normalized = NormalizedDraft::try_new(fact, &context, unit, graph, &self.document)?;
            match normalized {
                NormalizedDraft::Nonnumeric(value) => self.index.nonnumeric(*value)?,
                numeric => self.index.numeric(&numeric)?,
            }
        }
        for ordinal in 0..self.index.count("footnote")? {
            check_xbrl_cancelled(cancellation)?;
            self.refresh_retained()?;
            let mut footnote: FootnoteDraft = self
                .index
                .at("footnote", ordinal)?
                .ok_or(SecXbrlError::ParserInvariant)?;
            resolve_continuations(
                footnote.span,
                &mut footnote.continued_at,
                &mut footnote.text,
                &mut footnote.continuation_chain,
                &self.index,
                &mut self.retained_output,
                self.limits.string_bytes(),
                cancellation,
            )?;
            let relationships = self.index.incident(&footnote.id)?;
            let value = model::XbrlFootnoteOccurrence {
                occurrence_id: SourceIdentifier::try_from(footnote.id)?,
                accession: self.document.accession.clone(),
                language: XbrlText::try_from(footnote.language)?,
                role: SourceIdentifier::try_from(footnote.role)?,
                title: footnote.title.map(XbrlText::try_from).transpose()?,
                lexical_value: XbrlText::try_from(footnote.text)?,
                source_payload: self.document.source_payload.clone(),
                occurrence_relationships: XbrlOccurrenceRelationships::try_new(
                    None,
                    Vec::new(),
                    footnote
                        .continuation_chain
                        .into_iter()
                        .map(SourceIdentifier::try_from)
                        .collect::<Result<Vec<_>, _>>()?,
                    relationships,
                )?,
            };
            self.index.insert(
                "footnote_output",
                value.occurrence_id().as_str(),
                ordinal,
                &value,
            )?;
        }
        self.index.classify(cancellation)?;
        self.index.seal(cancellation)?;
        Ok(IndexedXbrlDocument {
            numeric_count: self.index.count("numeric")?,
            nonnumeric_count: self.index.count("nonnumeric")?,
            footnote_count: self.index.count("footnote_output")?,
            peak_retained_bytes: self
                .retained_output
                .peak
                .max(self.index.working_set_bytes()?),
            index: self.index,
            document: self.document,
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

#[derive(serde::Serialize, serde::Deserialize)]
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

    fn occurrence_context(
        &self,
    ) -> Result<std::sync::Arc<model::XbrlOccurrenceContext>, SecXbrlError> {
        Ok(std::sync::Arc::new(model::XbrlOccurrenceContext {
            entity: market_squawk_domain::XbrlEntity::try_new(
                self.entity_scheme
                    .as_deref()
                    .ok_or(SecXbrlError::IncompleteContext)?,
                self.entity_value
                    .as_deref()
                    .ok_or(SecXbrlError::IncompleteContext)?,
            )?,
            period: self.period()?,
            dimensions: self.dimensions.clone(),
            context_graph: XbrlContextGraph::try_new(self.graph_events.clone())?,
        }))
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

fn resolve_continuations(
    span: ElementSpan,
    continued_at: &mut Option<String>,
    text: &mut String,
    chain: &mut Vec<String>,
    continuations: &staging::FilingIndex,
    budget: &mut RetainedOutputBudget,
    string_limit: usize,
    cancellation: &CancellationToken,
) -> Result<(), SecXbrlError> {
    let mut next = continued_at.take();
    let mut seen = BTreeSet::new();
    let mut chain_spans = BTreeMap::new();
    if next.is_some() {
        span.admit_chain_member(&mut chain_spans, budget)?;
    }
    while let Some(continuation_id) = next {
        check_xbrl_cancelled(cancellation)?;
        budget.admit_btree_entry::<String, ()>(continuation_id.len())?;
        if !seen.insert(continuation_id.clone()) {
            return Err(SecXbrlError::ContinuationCycle);
        }
        let continuation: ContinuationDraft = continuations
            .get("continuation", &continuation_id)?
            .ok_or(SecXbrlError::UnknownContinuation)?;
        continuation
            .span
            .admit_chain_member(&mut chain_spans, budget)?;
        budget.admit(continuation.text.len())?;
        append_bounded(text, &continuation.text, string_limit)?;
        budget.admit_vec_entry::<String>(0)?;
        chain.push(continuation_id);
        budget.admit(
            continuation
                .continued_at
                .as_ref()
                .map_or(0, String::capacity),
        )?;
        next = continuation.continued_at.clone();
    }
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct FootnoteDraft {
    start_depth: usize,
    span: ElementSpan,
    id: String,
    language: String,
    role: String,
    title: Option<String>,
    continued_at: Option<String>,
    continuation_chain: Vec<String>,
    text: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct FactDraft {
    start_depth: usize,
    span: ElementSpan,
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

impl FactDraft {
    fn dynamic_bytes(&self) -> Result<usize, SecXbrlError> {
        checked_retained_sum([
            qname_dynamic_bytes(&self.concept)?,
            self.context_id.capacity(),
            self.unit_id.as_ref().map_or(0, String::capacity),
            self.occurrence_id.capacity(),
            self.parent_occurrence_id
                .as_ref()
                .map_or(0, String::capacity),
            self.format
                .as_ref()
                .map(qname_dynamic_bytes)
                .transpose()?
                .unwrap_or(0),
            self.language.as_ref().map_or(0, String::capacity),
            self.continued_at.as_ref().map_or(0, String::capacity),
            self.text.capacity(),
            self.continuation_chain.capacity() * size_of::<String>(),
            self.continuation_chain.iter().map(String::capacity).sum(),
        ])
    }
}

/// Inclusive XML preorder interval. Valid continuation chains contain disjoint
/// intervals: nesting in another chain is allowed, nesting within one is not.
#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct ElementSpan {
    start: usize,
    end: usize,
}

impl ElementSpan {
    const fn new(start: usize) -> Self {
        Self { start, end: start }
    }

    fn admit_chain_member(
        self,
        spans: &mut BTreeMap<usize, usize>,
        budget: &mut RetainedOutputBudget,
    ) -> Result<(), SecXbrlError> {
        if spans
            .range(..=self.start)
            .next_back()
            .is_some_and(|(_, end)| *end >= self.start)
            || spans
                .range(self.start..)
                .next()
                .is_some_and(|(start, _)| *start <= self.end)
        {
            return Err(SecXbrlError::NestedContinuation);
        }
        budget.admit_btree_entry::<usize, usize>(0)?;
        spans.insert(self.start, self.end);
        Ok(())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct ContinuationDraft {
    start_depth: usize,
    span: ElementSpan,
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
            .unwrap_or("http://www.xbrl.org/2003/arcrole/fact-footnote");
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

    fn try_new(attributes: &ResolvedAttributes) -> Result<Self, SecXbrlError> {
        Ok(Self {
            arcrole: attributes
                .unqualified("arcrole")
                .unwrap_or("http://www.xbrl.org/2003/arcrole/fact-footnote")
                .to_owned(),
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

/// Parser-only regression sharing the owning test's admitted taxonomy; these
/// in-memory documents never enter source publication or financial admission.
#[cfg(test)]
fn exercise_nested_continuations(document: XbrlDocumentContext) -> Result<(), SecXbrlError> {
    let xml = r#"<html xmlns="http://www.w3.org/1999/xhtml"
        xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
        xmlns:xbrli="http://www.xbrl.org/2003/instance"
        xmlns:dei="http://xbrl.sec.gov/dei/2025"><body>
        <xbrli:context id="c"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000320193</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:instant>2025-07-24</xbrli:instant></xbrli:period></xbrli:context>
        <ix:nonNumeric id="a" name="dei:EntityFileNumber" contextRef="c" continuedAt="ca">a</ix:nonNumeric>
        <ix:nonNumeric id="b" name="dei:EntityFileNumber" contextRef="c" continuedAt="cb">b</ix:nonNumeric>
        <ix:continuation id="ca">c<ix:continuation id="cb">d</ix:continuation><ix:exclude>ignored<ix:nonNumeric id="z" name="dei:EntityFileNumber" contextRef="c">z</ix:nonNumeric></ix:exclude>e</ix:continuation>
        </body></html>"#;
    let parsed = XbrlDocumentParser::parse_with_cancellation(
        xml.as_bytes(),
        SecParserLimits::production_defaults(),
        document.clone(),
        &CancellationToken::new(),
    )?;
    let indexed_cancellation = CancellationToken::new();
    let indexed = XbrlDocumentParser::parse_indexed_with_cancellation(
        xml.as_bytes(),
        SecParserLimits::production_defaults(),
        document.clone(),
        &indexed_cancellation,
    )?;
    assert_eq!(indexed.context_count()?, 1);
    assert_eq!(
        indexed.nonnumeric_count,
        parsed.nonnumeric_occurrences().len()
    );
    for (ordinal, expected) in parsed.nonnumeric_occurrences().iter().enumerate() {
        assert_eq!(indexed.nonnumeric_at(ordinal)?.as_ref(), Some(expected));
    }
    indexed_cancellation.cancel();
    assert!(matches!(
        indexed.nonnumeric_at(0),
        Err(SecXbrlError::Cancelled)
    ));
    let values = parsed
        .nonnumeric_occurrences()
        .iter()
        .map(|fact| (fact.occurrence_id().as_str(), fact.lexical_value().as_str()))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(values.get("a"), Some(&"acde"));
    assert_eq!(values.get("b"), Some(&"bd"));
    assert_eq!(values.get("z"), Some(&"z"));
    let invalid = xml
        .replace(" contextRef=\"c\" continuedAt=\"cb\"", " contextRef=\"c\"")
        .replace(
            "<ix:continuation id=\"ca\">",
            "<ix:continuation id=\"ca\" continuedAt=\"cb\">",
        );
    assert!(matches!(
        XbrlDocumentParser::parse_with_cancellation(
            invalid.as_bytes(),
            SecParserLimits::production_defaults(),
            document,
            &CancellationToken::new(),
        ),
        Err(SecXbrlError::NestedContinuation)
    ));
    Ok(())
}

/// The existing captured-taxonomy fixture owns this grammar and numeric-evidence regression.
#[cfg(test)]
fn exercise_number_word_transforms(document: XbrlDocumentContext) -> Result<(), SecXbrlError> {
    let fixed_zero_namespace = "http://www.xbrl.org/inlineXBRL/transformation/2020-02-12";
    let fixed_zero = XbrlQualifiedName::try_new("ixt:fixed-zero", fixed_zero_namespace)?;
    // TSLA 2026-06-30 uses all four nonempty spellings; the registry admits any string.
    for lexical in ["no", "No", "immaterial", "—", "", " \t123\n"] {
        assert_eq!(transform_numeric(lexical, Some(&fixed_zero))?, "0");
    }
    for unsupported in [
        XbrlQualifiedName::try_new("ixt:fixed-zero", "https://unrelated.test")?,
        XbrlQualifiedName::try_new("ixt:unknown", fixed_zero_namespace)?,
    ] {
        assert!(matches!(
            transform_numeric("no", Some(&unsupported)),
            Err(SecXbrlError::UnsupportedTransform)
        ));
    }
    let format = XbrlQualifiedName::try_new(
        "sec:numwordsen",
        "http://www.sec.gov/inlineXBRL/transformation/2015-08-31",
    )?;
    // SEC registry examples plus the magnitude, separator and zero alternatives. Values are
    // independent decimal literals; the arithmetic must never bypass the lexical grammar.
    for (lexical, expected) in [
        ("No", "0"),
        ("nil", "0"),
        (" Zero ", "0"),
        ("three", "3"),
        (" One Hundred and Twenty One ", "121"),
        ("nineteen hundred forty-four", "1944"),
        ("Seventy Thousand and one", "70001"),
        (
            "eighteen million three hundred thousand and fifty-one",
            "18300051",
        ),
        ("one\u{a0}hundred", "100"),
        ("one million, \tthree", "1000003"),
        ("one million\u{a0}", "1000000"),
        (
            "one quintillion two quadrillion three trillion four billion five million six thousand seven",
            "1002003004005006007",
        ),
        ("nineteen hundred quintillion", "1900000000000000000000"),
    ] {
        assert_eq!(
            transform_numeric(lexical, Some(&format))?,
            expected,
            "{lexical}"
        );
    }
    // The registry lists these exact dashes, not every Unicode dash or minus character.
    for dash in
        "-\u{058a}\u{05be}\u{2010}\u{2011}\u{2012}\u{2013}\u{2014}\u{2015}\u{fe58}\u{fe63}\u{ff0d}"
            .chars()
    {
        assert_eq!(
            transform_numeric(&format!("fifty{dash}one"), Some(&format))?,
            "51"
        );
    }
    for lexical in [
        "",
        "   ",
        "\u{a0}three",
        "three\u{a0}",
        "THREE",
        "tHree",
        "one one",
        "fiftyone",
        "fifty_one",
        "fifty−one",
        "zero hundred",
        "twenty hundred",
        "one hundred And one",
        "one million and one",
        "one thousand one million",
        "one thousand,",
        "one million,",
        "one million, \u{a0}",
        "four gazillion",
        "one sextillion",
        "one and 01/100",
        "two-thirds",
        "3",
        "negative three",
    ] {
        assert!(
            matches!(
                transform_numeric(lexical, Some(&format)),
                Err(SecXbrlError::InvalidNumericFact)
            ),
            "invalid lexical input: {lexical:?}",
        );
    }
    let unrelated = XbrlQualifiedName::try_new("sec:numwordsen", "https://unrelated.test")?;
    assert!(matches!(
        transform_numeric("three", Some(&unrelated)),
        Err(SecXbrlError::UnsupportedTransform)
    ));

    let xml = r#"<html xmlns="http://www.w3.org/1999/xhtml"
        xmlns:ix="http://www.xbrl.org/2013/inlineXBRL"
        xmlns:xbrli="http://www.xbrl.org/2003/instance"
        xmlns:us-gaap="http://fasb.org/us-gaap/2026"
        xmlns:msft="http://www.microsoft.com/20260630"
        xmlns:iso4217="http://www.xbrl.org/2003/iso4217"
        xmlns:ixt="http://www.xbrl.org/inlineXBRL/transformation/2020-02-12"
        xmlns:sec="http://www.sec.gov/inlineXBRL/transformation/2015-08-31"><body>
        <xbrli:context id="annual"><xbrli:entity><xbrli:identifier scheme="http://www.sec.gov/CIK">0000789019</xbrli:identifier></xbrli:entity><xbrli:period><xbrli:startDate>2025-07-01</xbrli:startDate><xbrli:endDate>2026-06-30</xbrli:endDate></xbrli:period></xbrli:context>
        <xbrli:unit id="segments"><xbrli:measure>msft:Segment</xbrli:measure></xbrli:unit>
        <xbrli:unit id="dollars"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
        <ix:nonFraction id="segments-fact" name="us-gaap:NumberOfReportableSegments" contextRef="annual" unitRef="segments" decimals="0" format="sec:numwordsen">three</ix:nonFraction>
        <ix:nonFraction id="scaled-fact" name="us-gaap:NetIncomeLoss" contextRef="annual" unitRef="dollars" decimals="2" format="sec:numwordsen" scale="-2" sign="-"> nineteen hundred forty-four </ix:nonFraction>
        <ix:nonFraction id="zero-fact" name="us-gaap:PreferredStockValue" contextRef="annual" unitRef="dollars" decimals="INF" format="ixt:fixed-zero" scale="6" sign="-"> no </ix:nonFraction>
        </body></html>"#;
    let parsed = XbrlDocumentParser::parse_with_cancellation(
        xml.as_bytes(),
        SecParserLimits::production_defaults(),
        document.clone(),
        &CancellationToken::new(),
    )?;
    let indexed = XbrlDocumentParser::parse_indexed_with_cancellation(
        xml.as_bytes(),
        SecParserLimits::production_defaults(),
        document.clone(),
        &CancellationToken::new(),
    )?;
    assert_eq!(parsed.numeric_facts().len(), 3);
    assert_eq!(indexed.numeric_count, 3);
    for (ordinal, expected) in parsed.numeric_facts().iter().enumerate() {
        assert_eq!(indexed.numeric_at(ordinal)?.as_ref(), Some(expected));
    }
    let segments = &parsed.numeric_facts()[0];
    assert_eq!(segments.value(), Decimal::from(3));
    assert_eq!(segments.evidence().lexical_value().as_str(), "three");
    assert_eq!(segments.evidence().context_id().as_str(), "annual");
    assert_eq!(
        segments.evidence().unit().source_identifier()?.as_str(),
        "msft:Segment"
    );
    assert_eq!(segments.evidence().entity().value().as_str(), "0000789019");
    assert_eq!(
        segments.evidence().period(),
        XbrlPeriod::duration(parse_date("2025-07-01")?, parse_date("2026-06-30")?,)?
    );
    assert_eq!(
        serde_json::to_value(segments.evidence())?["transformed_lexeme"],
        "3"
    );
    let scaled = &parsed.numeric_facts()[1];
    assert_eq!(scaled.value(), Decimal::new(-1944, 2));
    assert_eq!(
        scaled.evidence().lexical_value().as_str(),
        " nineteen hundred forty-four "
    );
    assert_eq!(scaled.evidence().inline_scale(), Some(-2));
    assert_eq!(scaled.evidence().inline_sign(), Some(XbrlSign::Negative));
    assert_eq!(
        serde_json::to_value(scaled.evidence())?["transformed_lexeme"],
        "1944"
    );
    assert_eq!(scaled.evidence().normalized_value()?, scaled.value());
    let zero = &parsed.numeric_facts()[2];
    assert_eq!(zero.value(), Decimal::ZERO);
    assert_eq!(zero.evidence().lexical_value().as_str(), " no ");
    assert_eq!(zero.evidence().inline_scale(), Some(6));
    assert_eq!(zero.evidence().inline_sign(), Some(XbrlSign::Negative));
    assert_eq!(
        serde_json::to_value(zero.evidence())?["transformed_lexeme"],
        "0"
    );
    assert_eq!(zero.evidence().normalized_value()?, zero.value());
    // The caller must not trim away invalid boundary NBSP before validating the transform.
    for invalid in [
        xml.replace(">three<", ">\u{a0}three<"),
        xml.replace("scale=\"-2\"", "scale=\"28\""),
        xml.replace("scale=\"6\"", "scale=\"29\""),
        xml.replace("scale=\"6\" sign=\"-\"", "scale=\"6\" sign=\"invalid\""),
    ] {
        assert!(matches!(
            XbrlDocumentParser::parse_with_cancellation(
                invalid.as_bytes(),
                SecParserLimits::production_defaults(),
                document.clone(),
                &CancellationToken::new(),
            ),
            Err(SecXbrlError::InvalidNumericFact)
        ));
    }
    Ok(())
}
