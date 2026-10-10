//! Current issuer/security links backed by retained submissions and official listing evidence.

use std::fmt::Write as _;
use std::time::Instant;

use market_squawk_data::{
    CompanySecurityIdentityCatalogError, ListingReferenceError, ListingReferenceFileKind,
    ListingReferenceRecord, sec_listing_exchange_matches_venue,
};
use market_squawk_domain::{
    CommonEquitySuitability, CompanyIdentitySurface, CompanySecurityIdentityLink,
    CompanySecurityIdentityLinkInput, CompanySecurityKind, CompanySecurityLinkTransition,
    CompanySecurityRelationshipKind, CompanySecurityResolutionBasis, EffectiveInterval,
    IdentifierEntitlement, IdentifierRightsPolicyReference, InstrumentId, SchemaVersion, SourceId,
    SourceIdentifier,
};
use tokio_util::sync::CancellationToken;

use super::{
    CompanySecurityResolutionAuthority, CompanySecurityResolutionError, check_operation,
    ensure_company_parent_times, ensure_interval_covers_authorization, map_company_catalog_error,
    map_market_catalog_error, system_timestamp, validate_market_record,
};

impl CompanySecurityResolutionAuthority {
    /// Attempts current relationships for every uniquely resolved retained issuer association.
    ///
    /// Returns the number of newly published surface events, not the number of suitable stocks.
    /// Missing, ambiguous, revoked and unverified candidates remain unresolved by catalog reads.
    pub(crate) fn ensure_company_listing_relationships(
        &self,
        company_source: &SourceId,
        cik: &SourceIdentifier,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<usize, CompanySecurityResolutionError> {
        check_operation(deadline, cancellation)?;
        let parent = self
            .company_identities
            .exact_current(
                company_source,
                cik,
                CompanyIdentitySurface::SecSubmissions,
                deadline,
                cancellation,
            )
            .map_err(map_company_catalog_error);
        let submissions = match parent {
            Ok(Some((submissions, _, _))) => submissions,
            Ok(None) => return Ok(0),
            Err(error) if unresolved_candidate(&error) => return Ok(0),
            Err(error) => return Err(error),
        };
        let now = system_timestamp()?;
        let mut published = 0;
        for association in submissions.associations() {
            check_operation(deadline, cancellation)?;
            let candidates = self
                .market_instruments
                .resolve_exact_as_of(association.ticker(), now, now, deadline, cancellation)
                .map_err(map_market_catalog_error)?;
            let [candidate] = candidates.matches() else {
                continue;
            };
            if candidates.has_more() {
                continue;
            }
            let definition = candidate.record().definition();
            let mut mappings = definition.venue_mappings().iter().filter(|mapping| {
                mapping.venue_symbol().as_str() == association.ticker()
                    && sec_listing_exchange_matches_venue(
                        association.exchange(),
                        mapping.venue_id().as_str(),
                    )
            });
            let Some(mapping) = mappings.next() else {
                continue;
            };
            if mappings.next().is_some() {
                continue;
            }
            let listing = match self.listings.exact_current(
                mapping.venue_symbol().as_str(),
                mapping.venue_id(),
                deadline,
                cancellation,
            ) {
                Ok(Some(listing)) => listing,
                Ok(None) => continue,
                Err(error) => {
                    let error = map_listing_error(error);
                    if unresolved_candidate(&error) {
                        continue;
                    }
                    return Err(error);
                }
            };
            match self.ensure_source_qualified_listing(
                company_source,
                cik,
                definition.instrument_id(),
                &listing,
                deadline,
                cancellation,
            ) {
                Ok(count) => published += count,
                Err(error) if unresolved_candidate(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(published)
    }

    /// Refreshes available submissions/facts/filing surfaces for one exact selected security.
    ///
    /// Catalog publication validates classification and every retained parent atomically. Explicit
    /// decisions are never replaced here. Callers must read catalog selection after this attempt;
    /// zero new events may mean unchanged authority, an absent facts surface, or a preserved decision.
    pub(crate) fn ensure_source_qualified_listing(
        &self,
        company_source: &SourceId,
        cik: &SourceIdentifier,
        instrument: InstrumentId,
        listing: &ListingReferenceRecord,
        deadline: Instant,
        cancellation: &CancellationToken,
    ) -> Result<usize, CompanySecurityResolutionError> {
        check_operation(deadline, cancellation)?;
        let (submissions, submissions_digest, submissions_completed_at) = self
            .company_identities
            .exact_current(
                company_source,
                cik,
                CompanyIdentitySurface::SecSubmissions,
                deadline,
                cancellation,
            )
            .map_err(map_company_catalog_error)?
            .ok_or(CompanySecurityResolutionError::ParentUnavailable)?;
        let mut associations = submissions.associations().iter().filter(|association| {
            association.ticker() == listing.provider_symbol()
                && sec_listing_exchange_matches_venue(
                    association.exchange(),
                    listing.listing_venue().as_str(),
                )
        });
        let association = associations
            .next()
            .ok_or(CompanySecurityResolutionError::ParentUnavailable)?;
        if associations.next().is_some() {
            return Err(CompanySecurityResolutionError::AmbiguousCandidates);
        }
        let basis = CompanySecurityResolutionBasis::SourceQualifiedListing {
            submissions_observation_digest: submissions_digest,
            listing_source_id: listing.generation().source_id().clone(),
            listing_dataset_id: listing.generation().dataset().clone(),
            listing_generation_digest: listing.generation().generation_digest(),
            listing_file_kind: identifier(match listing.source_file().kind() {
                ListingReferenceFileKind::NasdaqListed => "nasdaq_listed",
                ListingReferenceFileKind::OtherListed => "other_listed",
            })?,
            listing_row_number: listing.provider_row_number(),
            listing_record_digest: listing.record_digest(),
            listing_venue: listing.listing_venue().clone(),
            listing_symbol: identifier(listing.provider_symbol())?,
            sec_ticker: identifier(association.ticker())?,
            sec_exchange: identifier(association.exchange())?,
            classification_evidence: listing.record_payload_evidence().clone(),
            ruleset: identifier("sec-submissions-official-common-stock-v1")?,
        };
        let market = self
            .market_instruments
            .latest(instrument, deadline, cancellation)
            .map_err(map_market_catalog_error)?
            .ok_or(CompanySecurityResolutionError::ParentUnavailable)?;
        validate_market_record(instrument, &market)?;
        let interval = market.definition().effective_interval();
        let rights = listing_relationship_rights(listing)?;
        let mut published = 0;
        for surface in [
            CompanyIdentitySurface::SecSubmissions,
            CompanyIdentitySurface::SecCompanyFacts,
            CompanyIdentitySurface::SecFilingXbrl,
        ] {
            let current = self.read_relationship_state(
                company_source,
                cik,
                surface,
                instrument,
                deadline,
                cancellation,
            )?;
            if let Some(record) = &current.record {
                if record.link().transition().is_revocation() {
                    return Err(CompanySecurityResolutionError::RelationshipAlreadyRevoked);
                }
                if !matches!(
                    record.link().resolution_basis(),
                    CompanySecurityResolutionBasis::SourceQualifiedListing { .. }
                ) {
                    continue;
                }
            }
            let parent = match surface {
                CompanyIdentitySurface::SecSubmissions => Some((
                    submissions.clone(),
                    submissions_digest,
                    submissions_completed_at,
                )),
                CompanyIdentitySurface::SecCompanyFacts | CompanyIdentitySurface::SecFilingXbrl => {
                    self.company_identities
                        .exact_current(company_source, cik, surface, deadline, cancellation)
                        .map_err(map_company_catalog_error)?
                }
            };
            let Some((company, company_digest, completed_at)) = parent else {
                continue;
            };
            let now = system_timestamp()?;
            ensure_interval_covers_authorization(interval, now, interval.ends_at())?;
            ensure_company_parent_times(
                company.received_at(),
                company
                    .availability()
                    .conservative_available_at()
                    .ok_or(CompanySecurityResolutionError::ParentUnavailable)?,
                company.ingested_at(),
                completed_at,
                now,
            )?;
            if current.record.as_ref().is_some_and(|record| {
                record.link().company_observation_digest() == company_digest
                    && record.link().market_instrument_revision_digest() == market.revision_digest()
                    && record.link().resolution_basis() == &basis
                    && record.link().relationship_evidence_rights() == &rights
            }) {
                continue;
            }
            let transition =
                current
                    .record
                    .as_ref()
                    .map_or(CompanySecurityLinkTransition::Initial, |record| {
                        CompanySecurityLinkTransition::Supersedes {
                            previous_link_digest: record.link_digest(),
                        }
                    });
            let link = CompanySecurityIdentityLink::try_new(CompanySecurityIdentityLinkInput {
                schema_version: SchemaVersion::CURRENT,
                company_source_id: company_source.clone(),
                provider_company_id: cik.clone(),
                company_surface: surface,
                company_observation_digest: company_digest,
                instrument_id: instrument,
                market_instrument_revision_digest: market.revision_digest(),
                security_kind: CompanySecurityKind::CommonEquity,
                relationship_kind: CompanySecurityRelationshipKind::Issuer,
                common_equity_suitability: CommonEquitySuitability::SuitableIssuerCommonEquity,
                resolution_basis: basis.clone(),
                relationship_evidence_rights: rights.clone(),
                effective_interval: EffectiveInterval::new(now, interval.ends_at())
                    .map_err(|_| CompanySecurityResolutionError::ParentUnavailable)?,
                available_at: now,
                ingested_at: now,
                transition,
            })
            .map_err(CompanySecurityResolutionError::Domain)?;
            self.publisher
                .publish(link, deadline, cancellation)
                .map_err(map_company_catalog_error)?;
            published += 1;
        }
        Ok(published)
    }
}

fn identifier(value: &str) -> Result<SourceIdentifier, CompanySecurityResolutionError> {
    SourceIdentifier::try_from(value).map_err(|_| CompanySecurityResolutionError::InvalidRequest)
}

fn listing_relationship_rights(
    listing: &ListingReferenceRecord,
) -> Result<IdentifierRightsPolicyReference, CompanySecurityResolutionError> {
    // The retained generation already carries admitted display/persistence rights. Bind this
    // relationship-only use to that exact decision rather than granting rights to financial rows.
    let mut policy = String::from("listing-relationship:");
    for byte in listing.generation().rights_id() {
        write!(&mut policy, "{byte:02x}").map_err(|_| CompanySecurityResolutionError::Encoding)?;
    }
    Ok(IdentifierRightsPolicyReference::new(
        identifier(&policy)?,
        IdentifierEntitlement::LicensedInternalUse,
        listing.source_file().source_reference().clone(),
    ))
}

fn map_listing_error(error: ListingReferenceError) -> CompanySecurityResolutionError {
    match error {
        ListingReferenceError::Cancelled => CompanySecurityResolutionError::Cancelled,
        ListingReferenceError::DeadlineExceeded => CompanySecurityResolutionError::DeadlineExceeded,
        ListingReferenceError::AuthorityUnavailable => {
            CompanySecurityResolutionError::AuthorityUnavailable
        }
        error => CompanySecurityResolutionError::CompanyCatalog(
            CompanySecurityIdentityCatalogError::Listing(error),
        ),
    }
}

fn unresolved_candidate(error: &CompanySecurityResolutionError) -> bool {
    matches!(
        error,
        CompanySecurityResolutionError::ParentUnavailable
            | CompanySecurityResolutionError::ParentDrift
            | CompanySecurityResolutionError::AmbiguousParent
            | CompanySecurityResolutionError::AmbiguousCandidates
            | CompanySecurityResolutionError::AmbiguousCurrentRelationship
            | CompanySecurityResolutionError::RelationshipAlreadyRevoked
            | CompanySecurityResolutionError::CompanyCatalog(
                CompanySecurityIdentityCatalogError::UnverifiedIdentityAuthority
                    | CompanySecurityIdentityCatalogError::Listing(
                        ListingReferenceError::RightsUnavailable
                            | ListingReferenceError::SourceRevisionUnavailable
                            | ListingReferenceError::SupersededGeneration
                    )
            )
    )
}
