#[cfg(test)]
mod stream_key_tests {
    use std::num::NonZeroU16;
    use std::mem::size_of;
    use std::collections::HashSet;

    use market_squawk_domain::{
        AuthorizationBasis, CoverageConsolidation, CoverageDelay, DataQuality, DeliveryEvidence,
        DigestAlgorithm, EffectiveInterval, EvidenceDigest, ExactPayloadEvidence, InstrumentId,
        IntegrityRule, LiveEventClass, MarketDepth, MetadataRevision, ProviderChannel,
        ProviderProduct, RuleVersion, SequenceValidationRule, SnapshotApplicability, SourceId,
        SourceIdentifier, Timestamp, VenueId, VersionPinnedSourceLocator,
    };

    use super::{
        CurrentBatchKey, CurrentCoveragePolicy, CurrentLivePolicy, CurrentProviderObservation,
        CurrentStreamKey, RegistryError, current_authority_shared_allocation_charge,
        current_routed_batch_retained_bytes,
    };
    use crate::{
        AuthorizationGrant, AuthorizationHealth, AuthorizationMode, ChecksumAlgorithm,
        ChecksumBookScope, ChecksumValidationProfile, CoverageHealth, FreshnessPolicy,
        LiveCoverageRule, LiveProtocolProfile, ProviderNumericPolicy,
        SemanticInterpretationProfile, SequenceValidationProfile,
    };

    fn key(
        source: &str,
        venue: &str,
        instrument: &str,
        product: &str,
        channel: &str,
    ) -> Result<CurrentStreamKey, Box<dyn std::error::Error>> {
        Ok(CurrentStreamKey {
            source_id: SourceId::try_from(source)?,
            venue: VenueId::try_from(venue)?,
            instrument: instrument.parse::<InstrumentId>()?,
            provider_product: ProviderProduct::new(SourceIdentifier::try_from(product)?),
            provider_channel: ProviderChannel::new(SourceIdentifier::try_from(channel)?),
        })
    }

    #[test]
    fn hash_identity_separates_all_five_dimensions() -> Result<(), Box<dyn std::error::Error>> {
        let first_instrument = "018f0000-0000-7000-8000-000000000001";
        let second_instrument = "018f0000-0000-7000-8000-000000000002";
        let keys = HashSet::from([
            key("kraken-primary", "kraken", first_instrument, "BTC/USD", "book")?,
            key("kraken-secondary", "kraken", first_instrument, "BTC/USD", "book")?,
            key("kraken-primary", "coinbase", first_instrument, "BTC/USD", "book")?,
            key("kraken-primary", "kraken", second_instrument, "BTC/USD", "book")?,
            key("kraken-primary", "kraken", first_instrument, "XBT/USD", "book")?,
            key("kraken-primary", "kraken", first_instrument, "BTC/USD", "level3")?,
        ]);

        assert_eq!(keys.len(), 6);
        Ok(())
    }

    #[test]
    fn routed_batch_charges_shared_authority_and_frame_allocations_once()
    -> Result<(), Box<dyn std::error::Error>> {
        const PER_OBSERVATION_DYNAMIC: usize = 137;
        const SECOND_OBSERVATION_DYNAMIC: usize = 211;
        let one = current_routed_batch_retained_bytes(53, 1, PER_OBSERVATION_DYNAMIC, 97, 101)?;
        let two = current_routed_batch_retained_bytes(
            53,
            2,
            PER_OBSERVATION_DYNAMIC + SECOND_OBSERVATION_DYNAMIC,
            97,
            101,
        )?;

        assert_eq!(
            two.checked_sub(one),
            Some(size_of::<CurrentProviderObservation>() + SECOND_OBSERVATION_DYNAMIC)
        );
        Ok(())
    }

    #[test]
    fn batch_key_charges_retained_venue_capacity() -> Result<(), Box<dyn std::error::Error>> {
        let mut venue = String::with_capacity(VenueId::MAX_LENGTH);
        venue.push('x');
        let key = CurrentBatchKey {
            venue: VenueId::try_from(venue)?,
            instrument: "018f0000-0000-7000-8000-000000000001".parse()?,
        };

        assert!(key.dynamic_retained_bytes() >= VenueId::MAX_LENGTH);
        let without_key = current_routed_batch_retained_bytes(0, 1, 0, 0, 0)?;
        let with_key =
            current_routed_batch_retained_bytes(key.dynamic_retained_bytes(), 1, 0, 0, 0)?;
        assert_eq!(
            with_key.checked_sub(without_key),
            Some(key.dynamic_retained_bytes())
        );
        Ok(())
    }

    #[test]
    fn authority_charge_adds_budget_allocation_exactly_once() -> Result<(), RegistryError> {
        const SESSION: usize = 101;
        const CAPTURE: usize = 103;
        const BUDGET: usize = 107;
        let without_budget = current_authority_shared_allocation_charge(SESSION, CAPTURE, 0, 0)?;
        let with_budget =
            current_authority_shared_allocation_charge(SESSION, CAPTURE, BUDGET, 0)?;

        assert_eq!(with_budget.checked_sub(without_budget), Some(BUDGET));
        Ok(())
    }

    #[test]
    fn authority_charge_adds_clock_allocation_exactly_once() -> Result<(), RegistryError> {
        const SESSION: usize = 101;
        const CAPTURE: usize = 103;
        const CLOCK: usize = 109;
        let without_clock = current_authority_shared_allocation_charge(SESSION, CAPTURE, 0, 0)?;
        let with_clock =
            current_authority_shared_allocation_charge(SESSION, CAPTURE, 0, CLOCK)?;

        assert_eq!(with_clock.checked_sub(without_clock), Some(CLOCK));
        Ok(())
    }

    #[test]
    fn maximum_policy_variant_charges_every_nested_identifier_allocation()
    -> Result<(), Box<dyn std::error::Error>> {
        fn identifier(character: char) -> Result<SourceIdentifier, Box<dyn std::error::Error>> {
            SourceIdentifier::try_from(
                std::iter::repeat_n(character, SourceIdentifier::MAX_LENGTH).collect::<String>(),
            )
            .map_err(Into::into)
        }
        fn source_id(character: char) -> Result<SourceId, Box<dyn std::error::Error>> {
            SourceId::try_from(
                std::iter::repeat_n(character, SourceId::MAX_LENGTH).collect::<String>(),
            )
            .map_err(Into::into)
        }
        fn venue_id(character: char) -> Result<VenueId, Box<dyn std::error::Error>> {
            VenueId::try_from(
                std::iter::repeat_n(character, VenueId::MAX_LENGTH).collect::<String>(),
            )
            .map_err(Into::into)
        }
        fn rule(character: char) -> Result<IntegrityRule, Box<dyn std::error::Error>> {
            Ok(IntegrityRule::new(identifier(character)?, RuleVersion::new(1)?))
        }
        fn evidence(byte: u8) -> Result<ExactPayloadEvidence, Box<dyn std::error::Error>> {
            Ok(ExactPayloadEvidence::with_version_pinned_locator(
                EvidenceDigest::new(DigestAlgorithm::Sha256, [byte; 32]),
                VersionPinnedSourceLocator::new(identifier('x')?, identifier('y')?),
            ))
        }

        let policy = CurrentLivePolicy {
            stream_key: CurrentStreamKey {
                source_id: source_id('a')?,
                venue: venue_id('b')?,
                instrument: "018f0000-0000-7000-8000-000000000001".parse()?,
                provider_product: ProviderProduct::new(identifier('c')?),
                provider_channel: ProviderChannel::new(identifier('d')?),
            },
            quality_ceiling: DataQuality::DirectVerified,
            static_authorization: AuthorizationGrant::new(
                AuthorizationMode::PublicInterface,
                AuthorizationBasis::new(identifier('e')?),
                evidence(1)?,
                EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?,
            ),
            runtime_authorization: AuthorizationHealth::Valid {
                evidence: evidence(2)?,
                valid_until: Timestamp::from_unix_nanos(10),
            },
            coverage: CurrentCoveragePolicy {
                source_id: source_id('f')?,
                venue: venue_id('g')?,
                provider_product: ProviderProduct::new(identifier('h')?),
                provider_channel: ProviderChannel::new(identifier('i')?),
                event_class: LiveEventClass::Trade,
                depth: None,
                delay: CoverageDelay::RealTime,
                consolidation: CoverageConsolidation::SingleVenue,
                delivery: DeliveryEvidence::DirectVenue,
                evidence: evidence(3)?,
                effective_from: Timestamp::from_unix_nanos(0),
                effective_until: None,
                metadata_revision: MetadataRevision::new(identifier('j')?),
            },
            runtime_coverage: CoverageHealth::Sufficient {
                evidence: evidence(4)?,
                provider_product: ProviderProduct::new(identifier('k')?),
                provider_channel: ProviderChannel::new(identifier('l')?),
                valid_until: Timestamp::from_unix_nanos(10),
            },
            rule: LiveCoverageRule::try_new(
                LiveEventClass::Trade,
                None,
                SnapshotApplicability::NotApplicable {
                    metadata_rule: rule('m')?,
                },
            )?,
            protocol: LiveProtocolProfile::new(
                rule('n')?,
                SemanticInterpretationProfile::new(
                    rule('o')?,
                    rule('p')?,
                    rule('q')?,
                    rule('r')?,
                ),
                rule('s')?,
                SequenceValidationProfile::Provided {
                    rule: rule('t')?,
                    progression: SequenceValidationRule::Consecutive,
                },
                ChecksumValidationProfile::Provided {
                    rule: rule('u')?,
                    algorithm: ChecksumAlgorithm::Sha256,
                    canonicalization: identifier('v')?,
                    scope: identifier('w')?,
                    book_scope: Some(ChecksumBookScope::new(
                        MarketDepth::PriceLevel,
                        NonZeroU16::new(10),
                    )),
                },
                true,
                ProviderNumericPolicy::ExactDecimalLexeme,
            ),
            freshness: FreshnessPolicy::try_new(1, 1, 1, 1, 1)?,
            valid_until: Timestamp::from_unix_nanos(10),
            universe_evidence: Some(evidence(5)?),
        };

        let mut identifiers = vec![
            policy.stream_key.provider_product.as_source_identifier(),
            policy.stream_key.provider_channel.as_source_identifier(),
            policy.static_authorization.basis().as_source_identifier(),
            policy.coverage.provider_product.as_source_identifier(),
            policy.coverage.provider_channel.as_source_identifier(),
            policy.coverage.metadata_revision.as_source_identifier(),
        ];
        if let CoverageHealth::Sufficient {
            provider_product,
            provider_channel,
            ..
        } = &policy.runtime_coverage
        {
            identifiers.push(provider_product.as_source_identifier());
            identifiers.push(provider_channel.as_source_identifier());
        }
        if let SnapshotApplicability::NotApplicable { metadata_rule } =
            policy.rule.snapshot_applicability()
        {
            identifiers.push(metadata_rule.provider_rule());
        }
        let protocol = &policy.protocol;
        identifiers.extend([
            protocol.decoder_rule().provider_rule(),
            protocol.semantic_interpretation().aggressor_rule().provider_rule(),
            protocol.semantic_interpretation().auction_rule().provider_rule(),
            protocol
                .semantic_interpretation()
                .trading_status_rule()
                .provider_rule(),
            protocol
                .semantic_interpretation()
                .corporate_action_rule()
                .provider_rule(),
            protocol.timestamp_rule().provider_rule(),
        ]);
        match protocol.sequence() {
            SequenceValidationProfile::Unsupported { rule }
            | SequenceValidationProfile::Provided { rule, .. } => {
                identifiers.push(rule.provider_rule());
            }
        }
        match protocol.checksum() {
            ChecksumValidationProfile::Unsupported { rule } => {
                identifiers.push(rule.provider_rule());
            }
            ChecksumValidationProfile::Provided {
                rule,
                canonicalization,
                scope,
                ..
            } => {
                identifiers.extend([rule.provider_rule(), canonicalization, scope]);
            }
        }
        let source_ids = [&policy.stream_key.source_id, &policy.coverage.source_id];
        let venues = [&policy.stream_key.venue, &policy.coverage.venue];
        let exact_charge = identifiers
            .iter()
            .map(|value| value.retained_bytes())
            .chain(source_ids.iter().map(|value| value.retained_bytes()))
            .chain(venues.iter().map(|value| value.retained_bytes()))
            .sum::<usize>();
        let runtime_authorization_evidence = match &policy.runtime_authorization {
            AuthorizationHealth::Valid { evidence, .. } => evidence,
            AuthorizationHealth::Uninitialized | AuthorizationHealth::Invalid => {
                return Err("maximum policy must retain runtime authorization evidence".into());
            }
        };
        let runtime_coverage_evidence = match &policy.runtime_coverage {
            CoverageHealth::Sufficient { evidence, .. } => evidence,
            CoverageHealth::Uninitialized | CoverageHealth::Limited => {
                return Err("maximum policy must retain runtime coverage evidence".into());
            }
        };
        let universe_evidence = policy
            .universe_evidence
            .as_ref()
            .ok_or("maximum policy must retain universe evidence")?;
        let evidence_charge = [
            policy.static_authorization.evidence(),
            runtime_authorization_evidence,
            &policy.coverage.evidence,
            runtime_coverage_evidence,
            universe_evidence,
        ]
        .into_iter()
        .try_fold(0_usize, |bytes, evidence| {
            bytes.checked_add(
                evidence
                    .dynamic_retained_bytes()
                    .ok_or(RegistryError::RetainedSizeOverflow)?,
            )
            .ok_or(RegistryError::RetainedSizeOverflow)
        })?;
        assert_eq!(
            policy.deep_allocation_charge()?,
            exact_charge
                .checked_add(evidence_charge)
                .ok_or(RegistryError::RetainedSizeOverflow)?
        );
        Ok(())
    }
}
