//! Explicit native-identity evidence for registry unit and integration fixtures only.
//! This deterministic authority does not claim real catalog publication or provider evidence.

use super::sources;
use super::{TestResult, exact_evidence, source_identifier};
use market_squawk_domain::{
    EffectiveInterval, InstrumentId, MetadataRevision, SourceId, Timestamp, VenueId,
};
use sources::{ProviderIdentitySelectionEvidence, ProviderNativeIdentityRequest};

pub(crate) fn fixture_identity_authority(
    routes: &[(InstrumentId, &str)],
    selected_at: Timestamp,
) -> TestResult<(
    std::sync::Arc<dyn sources::CatalogProviderIdentityAuthority>,
    Vec<ProviderNativeIdentityRequest>,
)> {
    let validity = EffectiveInterval::new(Timestamp::from_unix_nanos(0), None)?;
    let mut selections = Vec::new();
    for (instrument, symbol) in routes {
        selections.push(std::sync::Arc::new(FixtureIdentitySelection(
            ProviderIdentitySelectionEvidence {
                native: ProviderNativeIdentityRequest {
                    namespace: SourceId::try_from("coinbase-advanced-trade")?,
                    provider_instrument_id: market_squawk_domain::ProviderInstrumentId::try_from(
                        *symbol,
                    )?,
                    instrument: *instrument,
                    venue: VenueId::try_from("coinbase")?,
                    venue_symbol: market_squawk_domain::VenueSymbol::try_from(*symbol)?,
                    knowledge_at: selected_at,
                    effective_at: selected_at,
                },
                definition_digest: exact_evidence(31).content_digest(),
                definition_sequence: 1,
                reference_revision: MetadataRevision::new(source_identifier(
                    "fixture-reference-v1",
                )?),
                reference_payload_digest: exact_evidence(32).content_digest(),
                definition_published_at: selected_at,
                definition_validity: validity,
                provider_revision: MetadataRevision::new(source_identifier("fixture-provider-v1")?),
                provider_payload_digest: exact_evidence(33).content_digest(),
                provider_validity: validity,
                resolution_digest: exact_evidence(34).content_digest(),
                selection_digest: exact_evidence(35).content_digest(),
            },
        )));
    }
    let requests = selections
        .iter()
        .map(|selected| selected.0.native.clone())
        .collect::<Vec<_>>();
    Ok((
        std::sync::Arc::new(FixtureIdentityCatalog(selections)),
        requests,
    ))
}

#[derive(Debug)]
struct FixtureIdentityCatalog(Vec<std::sync::Arc<FixtureIdentitySelection>>);

impl sources::CatalogProviderIdentityAuthority for FixtureIdentityCatalog {
    fn select_current(
        &self,
        request: &sources::ProviderNativeIdentityRequest,
        deadline: std::time::Instant,
        cancellation: &tokio_util::sync::CancellationToken,
    ) -> Result<std::sync::Arc<dyn sources::CurrentCatalogProviderIdentity>, sources::RegistryError>
    {
        use sources::RegistryError;
        if cancellation.is_cancelled() {
            return Err(RegistryError::ProviderIdentitySelectionCancelled);
        }
        if std::time::Instant::now() >= deadline {
            return Err(RegistryError::ProviderIdentitySelectionDeadlineExceeded);
        }
        let mut matches = self
            .0
            .iter()
            .filter(|selected| selected.0.native == *request);
        let selected = matches.next().ok_or(RegistryError::LiveScopeNotCovered)?;
        if matches.next().is_some() {
            return Err(RegistryError::LiveScopeNotCovered);
        }
        Ok(selected.clone())
    }
}

#[derive(Debug)]
struct FixtureIdentitySelection(sources::ProviderIdentitySelectionEvidence);

impl sources::CurrentCatalogProviderIdentity for FixtureIdentitySelection {
    fn evidence(&self) -> &sources::ProviderIdentitySelectionEvidence {
        &self.0
    }

    fn validate_at(&self, at: Timestamp) -> Result<(), sources::RegistryError> {
        if at < self.0.definition_published_at
            || [self.0.definition_validity, self.0.provider_validity]
                .into_iter()
                .any(|validity| {
                    at < validity.starts_at() || validity.ends_at().is_some_and(|end| at >= end)
                })
        {
            return Err(sources::RegistryError::StaleHandle);
        }
        Ok(())
    }

    fn retained_bytes(&self) -> Result<usize, sources::RegistryError> {
        std::mem::size_of::<Self>()
            .checked_add(
                self.0
                    .dynamic_retained_bytes()
                    .ok_or(sources::RegistryError::RetainedSizeOverflow)?,
            )
            .and_then(|bytes| bytes.checked_add(2 * std::mem::size_of::<usize>()))
            .ok_or(sources::RegistryError::RetainedSizeOverflow)
    }
}
