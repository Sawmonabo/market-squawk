//! Producer-derived valuation input construction and validation.

use super::activity::derive_market_activity;
use super::*;

impl ValuationInput {
    /// Derives a quoted-price input from genuine post-commit live receipts.
    ///
    /// The selected amount, instrument, venue, quality, access, provenance, and market-activity
    /// conclusion all come from the receipts and policy. The caller supplies only a receipt
    /// selection and accounting significance.
    ///
    /// # Errors
    ///
    /// Rejects empty/excessive receipt sets, incompatible markets, duplicate-only activity,
    /// invalid selection, or exact price conversion failure.
    pub fn from_committed_market(
        request: CommittedMarketInputRequest<'_>,
    ) -> Result<Self, FairValueError> {
        let CommittedMarketInputRequest {
            receipts,
            selected_index,
            selection,
            significance,
            account_id,
            measurement_at,
            ruleset,
            market_access_assessment,
        } = request;
        let activity_policy = ruleset.market_activity_policy();
        if receipts.is_empty()
            || receipts.len() > activity_policy.maximum_receipts
            || receipts.len() > HARD_MAX_ACTIVITY_RECEIPTS
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        let selected = receipts
            .get(selected_index)
            .ok_or(FairValueError::InvalidProducerEvidence)?;
        let terms = selected.execution_terms();
        let ticks = match (selected.price(), selection) {
            (QualifiedMarketPrice::Trade { price, .. }, MarketPriceSelection::Trade) => price,
            (
                QualifiedMarketPrice::Quote {
                    bid: Some(value), ..
                },
                MarketPriceSelection::Bid,
            ) => value.price(),
            (
                QualifiedMarketPrice::Quote {
                    ask: Some(value), ..
                },
                MarketPriceSelection::Ask,
            ) => value.price(),
            _ => return Err(FairValueError::InvalidProducerEvidence),
        };
        let decimal = ticks
            .checked_to_decimal(terms.price_tick())
            .map_err(|_| FairValueError::InvalidAmount)?;
        let scale = u8::try_from(terms.price_tick().as_decimal().scale())
            .map_err(|_| FairValueError::InvalidAmount)?;
        let amount = ValuationAmount::try_new(Money::new(decimal, terms.quote_currency()), scale)?;
        let (market_activity, activity_set_hash) =
            derive_market_activity(receipts, selected, measurement_at, activity_policy)?;
        let market_access = match market_access_assessment {
            Some(assessment) => {
                assessment.validate_for(
                    account_id,
                    selected.venue_id(),
                    selected.instrument_id(),
                    measurement_at,
                )?;
                assessment.conclusion()
            }
            None => MarketAccess::NotAssessed,
        };
        let accessible = selected.source_authorization() == SourceAuthorization::Authorized
            && selected.coverage_status() == CoverageStatus::Sufficient
            && selected.trading_status() == TradingStatus::Active;
        let qualification_current = selected.qualification_evaluated_at() <= measurement_at
            && measurement_at <= selected.qualification_valid_until();
        let verification =
            if selected.source_timestamp().is_some() && accessible && qualification_current {
                EvidenceVerification::Verified
            } else {
                EvidenceVerification::Unverified
            };
        let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
            source_id: selected.source_id().clone(),
            source_identifier: selected.source_identifier().clone(),
            payload_digest: selected.payload_digest(),
            origin: EvidenceOrigin::Market {
                venue_id: selected.venue_id().clone(),
                assessment_id: selected.assessment_id().as_source_identifier().clone(),
                binding_digest: selected.binding_digest(),
                canonical_state_digest: selected.canonical_state_digest().digest(),
                committed_state_revision: selected.committed_state_revision(),
                definition_revision: terms.definition_revision().get(),
                activity_policy_hash: activity_policy.hash().bytes(),
                activity_set_hash,
            },
            source_timestamp: selected.source_timestamp(),
            effective_at: None,
            published_at: None,
            available_at: Some(selected.available_at()),
            received_at: Some(selected.received_at()),
            qualification_evaluated_at: Some(selected.qualification_evaluated_at()),
            qualification_valid_until: Some(selected.qualification_valid_until()),
            ingested_at: selected.ingested_at(),
            verification,
        })?;
        Self::try_from_spec(ValuationInputSpec {
            subject_instrument_id: selected.instrument_id(),
            reference_instrument_id: selected.instrument_id(),
            relationship: InputInstrumentRelation::Identical,
            amount,
            significance,
            observability: InputObservability::QuotedPrice,
            adjustment: PriceAdjustment::None,
            market_activity,
            market_access,
            market_access_assessment: market_access_assessment.cloned(),
            data_quality: selected.recorded_quality(),
            evidence,
            use_assessment: None,
        })
    }

    /// Derives an observable input from one exact cell of a manifest-pinned query result.
    ///
    /// # Errors
    ///
    /// Rejects rows without an instrument identity or exact decimal conversion.
    pub fn from_research(
        value: &PinnedMonetaryValue,
        significance: InputSignificance,
    ) -> Result<Self, FairValueError> {
        let instrument_id = value
            .instrument_id()
            .ok_or(FairValueError::MissingProducerInstrument)?;
        let amount = ValuationAmount::try_new(
            Money::new(
                value.decimal().map_err(|_| FairValueError::InvalidAmount)?,
                value.currency(),
            ),
            value.scale(),
        )?;
        let verification = if value.available_at().is_some()
            && (value.source_timestamp().is_some() || value.effective_at().is_some())
        {
            EvidenceVerification::Verified
        } else {
            EvidenceVerification::Unverified
        };
        let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
            source_id: value.source_id().clone(),
            source_identifier: value.source_identifier().clone(),
            payload_digest: value.payload_digest(),
            origin: EvidenceOrigin::Research {
                manifest: value.manifest().clone(),
                object_graph_digest: value.object_graph_digest(),
                query_identity: value.query_identity(),
                result_digest: value.result_digest(),
                row: value.row(),
                revision: value.revision(),
            },
            source_timestamp: value.source_timestamp(),
            effective_at: value.effective_at(),
            published_at: value.published_at(),
            available_at: value.available_at(),
            received_at: Some(value.received_at()),
            qualification_evaluated_at: None,
            qualification_valid_until: None,
            ingested_at: value.ingested_at(),
            verification,
        })?;
        Self::try_from_spec(ValuationInputSpec {
            subject_instrument_id: instrument_id,
            reference_instrument_id: instrument_id,
            relationship: InputInstrumentRelation::Identical,
            amount,
            significance,
            observability: InputObservability::Observable,
            adjustment: PriceAdjustment::None,
            market_activity: MarketActivity::NotAssessed,
            market_access: MarketAccess::NotAssessed,
            market_access_assessment: None,
            data_quality: value.data_quality(),
            evidence,
            use_assessment: None,
        })
    }

    fn analytics_spec(
        value: &PinnedFeatureMonetaryValue,
        registry: &FeatureRegistry,
        significance: InputSignificance,
    ) -> Result<ValuationInputSpec, FairValueError> {
        let feature_key = FeatureKey::try_new(value.component_name(), value.component_version())
            .map_err(|_| FairValueError::InvalidProducerEvidence)?;
        let feature = registry
            .try_resolve(&feature_key, FeatureCompatibility::PointInTime)
            .map_err(|_| FairValueError::InvalidProducerEvidence)?;
        if feature.output_type() != FeatureOutputType::Money
            || feature.unit() != FeatureUnit::CurrencyAmount
            || value
                .unit()
                .is_some_and(|unit| unit.as_str() != "currency_amount")
        {
            return Err(FairValueError::InvalidProducerEvidence);
        }
        let amount = ValuationAmount::try_new(
            Money::new(
                value.decimal().map_err(|_| FairValueError::InvalidAmount)?,
                value.currency(),
            ),
            value.scale(),
        )?;
        let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
            source_id: SourceId::try_from("market-squawk.analytics")
                .map_err(|_| FairValueError::InvalidProducerEvidence)?,
            source_identifier: value.example_id().clone(),
            payload_digest: value.lineage_digest(),
            origin: EvidenceOrigin::Analytics {
                feature_key,
                semantic_digest: feature.semantic_digest().as_bytes(),
                manifest: value.manifest().clone(),
                object_graph_digest: value.object_graph_digest(),
                query_identity: value.query_identity(),
                result_digest: value.result_digest(),
                row: value.row(),
                revision: value.component_version().get(),
            },
            source_timestamp: Some(value.cutoff_at()),
            effective_at: Some(value.cutoff_at()),
            published_at: None,
            available_at: Some(value.cutoff_at()),
            received_at: None,
            qualification_evaluated_at: None,
            qualification_valid_until: None,
            ingested_at: value.cutoff_at(),
            verification: EvidenceVerification::Verified,
        })?;
        Ok(ValuationInputSpec {
            subject_instrument_id: value.instrument_id(),
            reference_instrument_id: value.instrument_id(),
            relationship: InputInstrumentRelation::Identical,
            amount,
            significance,
            observability: InputObservability::Observable,
            adjustment: PriceAdjustment::None,
            market_activity: MarketActivity::NotAssessed,
            market_access: MarketAccess::NotAssessed,
            market_access_assessment: None,
            data_quality: DataQuality::Modeled,
            evidence,
            use_assessment: None,
        })
    }

    /// Derives one input from an actual immutable portfolio revision and selected real position.
    ///
    /// The instrument argument is only a bounded selector; the input identity, quantity, amount,
    /// account, dataset, point-in-time evidence, and timestamps come from the revision object.
    ///
    /// # Errors
    ///
    /// Returns [`FairValueError::MissingProducerInstrument`] when the revision has no such
    /// position.
    pub fn from_portfolio_position(
        revision: &PortfolioRevision,
        instrument_id: InstrumentId,
        significance: InputSignificance,
    ) -> Result<Self, FairValueError> {
        let position = revision
            .position(instrument_id)
            .ok_or(FairValueError::MissingProducerInstrument)?;
        let revision_evidence = revision.evidence();
        let amount_money = position.market_value();
        let scale = u8::try_from(amount_money.amount().scale())
            .map_err(|_| FairValueError::InvalidAmount)?;
        let amount = ValuationAmount::try_new(amount_money, scale)?;
        let point_in_time_digest = portfolio_evidence_digest(revision);
        let token = revision.token();
        let source_identifier = digest_identifier("portfolio-revision-", token.bytes())?;
        let evidence = FairValueEvidence::try_from_parts(FairValueEvidenceParts {
            source_id: SourceId::try_from("market-squawk.portfolio")
                .map_err(|_| FairValueError::InvalidProducerEvidence)?,
            source_identifier,
            payload_digest: EvidenceDigest::new(DigestAlgorithm::Sha256, token.bytes()),
            origin: EvidenceOrigin::Portfolio {
                revision: token.bytes(),
                account_id: revision.account_id(),
                position_quantity: position.quantity(),
                point_in_time_digest,
            },
            source_timestamp: Some(revision_evidence.as_of()),
            effective_at: Some(revision_evidence.as_of()),
            published_at: None,
            available_at: Some(revision_evidence.as_of()),
            received_at: None,
            qualification_evaluated_at: None,
            qualification_valid_until: None,
            ingested_at: revision_evidence.as_of(),
            verification: EvidenceVerification::Verified,
        })?;
        Self::try_from_spec(ValuationInputSpec {
            subject_instrument_id: position.instrument_id(),
            reference_instrument_id: position.instrument_id(),
            relationship: InputInstrumentRelation::Identical,
            amount,
            significance,
            observability: InputObservability::Observable,
            adjustment: PriceAdjustment::None,
            market_activity: MarketActivity::NotAssessed,
            market_access: MarketAccess::NotAssessed,
            market_access_assessment: None,
            data_quality: DataQuality::Estimated,
            evidence,
            use_assessment: None,
        })
    }

    /// Applies an actor-attributed comparable, proxy, adjusted, or unobservable use assessment to
    /// a producer-derived research value.
    pub fn from_assessed_research(
        value: &PinnedMonetaryValue,
        significance: InputSignificance,
        assessment: InputUseAssessment,
    ) -> Result<Self, FairValueError> {
        Self::with_use_assessment(Self::from_research(value, significance)?, assessment)
    }

    /// Applies an actor-attributed non-Level-1 use assessment to a registered point-in-time
    /// analytical feature.
    pub fn from_assessed_analytics(
        value: &PinnedFeatureMonetaryValue,
        registry: &FeatureRegistry,
        significance: InputSignificance,
        assessment: InputUseAssessment,
    ) -> Result<Self, FairValueError> {
        let spec = Self::analytics_spec(value, registry, significance)?;
        assessment.validate_for(spec.reference_instrument_id, spec.evidence.ingested_at())?;
        Self::try_from_spec(ValuationInputSpec {
            subject_instrument_id: assessment.subject_instrument_id(),
            reference_instrument_id: spec.reference_instrument_id,
            relationship: assessment.relationship(),
            amount: spec.amount,
            significance: spec.significance,
            observability: assessment.observability(),
            adjustment: assessment.adjustment(),
            market_activity: spec.market_activity,
            market_access: spec.market_access,
            market_access_assessment: spec.market_access_assessment,
            data_quality: spec.data_quality,
            evidence: spec.evidence,
            use_assessment: Some(assessment),
        })
    }

    fn with_use_assessment(
        input: Self,
        assessment: InputUseAssessment,
    ) -> Result<Self, FairValueError> {
        if !matches!(
            input.evidence.origin(),
            EvidenceOrigin::Research { .. } | EvidenceOrigin::Analytics { .. }
        ) {
            return Err(FairValueError::InvalidInputAssessment);
        }
        assessment.validate_for(input.reference_instrument_id, input.evidence.ingested_at())?;
        Self::try_from_spec(ValuationInputSpec {
            subject_instrument_id: assessment.subject_instrument_id(),
            reference_instrument_id: input.reference_instrument_id,
            relationship: assessment.relationship(),
            amount: input.amount,
            significance: input.significance,
            observability: assessment.observability(),
            adjustment: assessment.adjustment(),
            market_activity: MarketActivity::NotAssessed,
            market_access: MarketAccess::NotAssessed,
            market_access_assessment: None,
            data_quality: input.data_quality,
            evidence: input.evidence,
            use_assessment: Some(assessment),
        })
    }

    /// Checks relationship invariants and derives an immutable content identity.
    ///
    /// # Errors
    ///
    /// Rejects an `Identical` relation across different identities or a non-identical relation
    /// that reuses the measured identity.
    pub(crate) fn try_from_spec(spec: ValuationInputSpec) -> Result<Self, FairValueError> {
        let same = spec.subject_instrument_id == spec.reference_instrument_id;
        if same != (spec.relationship == InputInstrumentRelation::Identical) {
            return Err(FairValueError::InvalidInstrumentRelationship);
        }
        if matches!(spec.evidence.origin(), EvidenceOrigin::Analytics { .. })
            && spec.use_assessment.is_none()
        {
            return Err(FairValueError::InvalidInputAssessment);
        }
        if (spec.market_access == MarketAccess::NotAssessed)
            != spec.market_access_assessment.is_none()
        {
            return Err(FairValueError::InvalidMarketAccessAssessment);
        }
        if let Some(assessment) = &spec.market_access_assessment {
            let EvidenceOrigin::Market { venue_id, .. } = spec.evidence.origin() else {
                return Err(FairValueError::InvalidMarketAccessAssessment);
            };
            if assessment.venue_id() != venue_id
                || assessment.instrument_id() != spec.reference_instrument_id
                || assessment.conclusion() != spec.market_access
            {
                return Err(FairValueError::InvalidMarketAccessAssessment);
            }
        }
        if let Some(assessment) = &spec.use_assessment
            && (!matches!(
                spec.evidence.origin(),
                EvidenceOrigin::Research { .. } | EvidenceOrigin::Analytics { .. }
            ) || assessment.subject_instrument_id() != spec.subject_instrument_id
                || assessment.relationship() != spec.relationship
                || assessment.observability() != spec.observability
                || assessment.adjustment() != spec.adjustment
                || assessment
                    .validate_for(spec.reference_instrument_id, spec.evidence.ingested_at())
                    .is_err())
        {
            return Err(FairValueError::InvalidInputAssessment);
        }
        let retained_bytes = checked_add(
            size_of::<Self>(),
            checked_add(
                spec.evidence.retained_bytes(),
                checked_add(
                    spec.use_assessment
                        .as_ref()
                        .map_or(0, InputUseAssessment::retained_bytes),
                    spec.market_access_assessment
                        .as_ref()
                        .map_or(0, ApprovedMarketAccess::retained_bytes),
                )?,
            )?,
        )?;
        let mut hash = CanonicalHasher::new(b"market-squawk/valuation-input/v1");
        hash.bytes(spec.subject_instrument_id.as_uuid().as_bytes());
        hash.bytes(spec.reference_instrument_id.as_uuid().as_bytes());
        hash.u8(relation_tag(spec.relationship));
        spec.amount.hash_into(&mut hash);
        hash.u8(significance_tag(spec.significance));
        hash.u8(observability_tag(spec.observability));
        hash.u8(adjustment_tag(spec.adjustment));
        hash.u8(activity_tag(spec.market_activity));
        hash.u8(access_tag(spec.market_access));
        match &spec.market_access_assessment {
            Some(value) => {
                hash.u8(1);
                hash.fixed(value.id().bytes());
            }
            None => hash.u8(0),
        }
        hash.u8(quality_tag(spec.data_quality));
        hash.fixed(spec.evidence.hash().bytes());
        match &spec.use_assessment {
            Some(value) => {
                hash.u8(1);
                hash.fixed(value.hash().bytes());
            }
            None => hash.u8(0),
        }
        Ok(Self {
            id: InputId(hash.finish()),
            subject_instrument_id: spec.subject_instrument_id,
            reference_instrument_id: spec.reference_instrument_id,
            relationship: spec.relationship,
            amount: spec.amount,
            significance: spec.significance,
            observability: spec.observability,
            adjustment: spec.adjustment,
            market_activity: spec.market_activity,
            market_access: spec.market_access,
            market_access_assessment: spec.market_access_assessment,
            data_quality: spec.data_quality,
            evidence: spec.evidence,
            use_assessment: spec.use_assessment,
            retained_bytes,
        })
    }

    /// Returns immutable input identity.
    pub const fn id(&self) -> InputId {
        self.id
    }

    /// Returns measured instrument identity.
    pub const fn subject_instrument_id(&self) -> InstrumentId {
        self.subject_instrument_id
    }

    /// Returns referenced instrument identity.
    pub const fn reference_instrument_id(&self) -> InstrumentId {
        self.reference_instrument_id
    }

    /// Returns instrument relationship.
    pub const fn relationship(&self) -> InputInstrumentRelation {
        self.relationship
    }

    /// Returns exact input amount.
    pub const fn amount(&self) -> ValuationAmount {
        self.amount
    }

    /// Returns input significance.
    pub const fn significance(&self) -> InputSignificance {
        self.significance
    }

    /// Returns accounting observability.
    pub const fn observability(&self) -> InputObservability {
        self.observability
    }

    /// Returns source-input adjustment.
    pub const fn adjustment(&self) -> PriceAdjustment {
        self.adjustment
    }

    /// Returns market activity conclusion.
    pub const fn market_activity(&self) -> MarketActivity {
        self.market_activity
    }

    /// Returns market access conclusion.
    pub const fn market_access(&self) -> MarketAccess {
        self.market_access
    }

    /// Returns the dual-approved access assessment when market access was assessed.
    pub const fn market_access_assessment(&self) -> Option<&ApprovedMarketAccess> {
        self.market_access_assessment.as_ref()
    }

    /// Returns independent data-quality classification.
    pub const fn data_quality(&self) -> DataQuality {
        self.data_quality
    }

    /// Returns immutable source evidence.
    pub const fn evidence(&self) -> &FairValueEvidence {
        &self.evidence
    }

    /// Returns the governed non-Level-1 use assessment when one was applied.
    pub const fn use_assessment(&self) -> Option<&InputUseAssessment> {
        self.use_assessment.as_ref()
    }

    pub(crate) const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

fn portfolio_evidence_digest(revision: &PortfolioRevision) -> [u8; 32] {
    let evidence = revision.evidence();
    let mut hash = CanonicalHasher::new(b"market-squawk/portfolio-valuation-evidence/v1");
    hash.fixed(revision.token().bytes());
    hash.bytes(evidence.dataset().dataset_id().as_str().as_bytes());
    hash.u64(evidence.dataset().manifest_version());
    hash.fixed(evidence.dataset().content_hash().bytes());
    hash.fixed(evidence.point_in_time_content().bytes());
    hash.fixed(evidence.point_in_time_audit().bytes());
    hash.u64(u64::try_from(evidence.sources().len()).unwrap_or(u64::MAX));
    for source in evidence.sources() {
        hash.bytes(source.as_str().as_bytes());
    }
    hash.finish()
}

fn digest_identifier(prefix: &str, digest: [u8; 32]) -> Result<SourceIdentifier, FairValueError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::new();
    value
        .try_reserve_exact(prefix.len().saturating_add(64))
        .map_err(|_| FairValueError::Arithmetic)?;
    value.push_str(prefix);
    for byte in digest {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    SourceIdentifier::try_from(value).map_err(|_| FairValueError::InvalidProducerEvidence)
}
