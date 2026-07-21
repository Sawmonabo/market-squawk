use std::error::Error;

use cap_std::{ambient_authority, fs::Dir};
use market_squawk_adapter_sec::{
    RawEvidenceStore, RetrievedCompanyFacts, RetrievedSubmissions, SecParserLimits,
    normalize_company_facts, normalize_filings,
};
use market_squawk_domain::{
    AvailabilityEvidence, EffectiveInterval, EvidenceDigest, InstrumentId, MetadataRevision,
    PayloadHashAlgorithm, ProviderIdentityEvidence, ProviderIdentityRecord,
    ProviderIdentityRecordInput, ProviderIdentityRegistry, ProviderInstrumentId,
    ResearchObservation, ResearchTemporalPrecision, SourceId, SourceIdentifier, Timestamp,
};
use uuid::Uuid;

#[test]
fn company_facts_resolve_cik_and_preserve_amendments_as_pit_revisions() -> Result<(), Box<dyn Error>>
{
    let source_id = SourceId::try_from("sec-edgar")?;
    let instrument_id = InstrumentId::try_from(Uuid::from_u128(7))?;
    let identities =
        ProviderIdentityRegistry::try_from_records(vec![ProviderIdentityRecord::new(
            ProviderIdentityRecordInput {
                instrument_id,
                source_id: source_id.clone(),
                provider_instrument_id: ProviderInstrumentId::try_from("0000320193")?,
                evidence: ProviderIdentityEvidence::from_content_digest(EvidenceDigest::new(
                    PayloadHashAlgorithm::Sha256,
                    [9; 32],
                )),
                source_timestamp: None,
                observed_at: Timestamp::from_unix_nanos(1),
                metadata_revision: MetadataRevision::new(SourceIdentifier::try_from("sec-id-v1")?),
                validity: EffectiveInterval::new(Timestamp::from_unix_nanos(1), None)?,
                supersedes: None,
            },
        )])?;
    let temporary = tempfile::tempdir()?;
    let store = RawEvidenceStore::new(Dir::open_ambient_dir(
        temporary.path(),
        ambient_authority(),
    )?);
    let retrieved = RetrievedCompanyFacts::import_exact_bytes(
        include_bytes!("../fixtures/company-facts.json"),
        &store,
        SecParserLimits::production_defaults(),
    )?;
    let ingested_at = retrieved.raw().received_at().checked_add_nanos(1)?;
    let observations = normalize_company_facts(&source_id, &identities, &retrieved, ingested_at)?;

    assert_eq!(observations.len(), 3);
    let mut asset_revisions = observations
        .iter()
        .filter_map(|observation| match observation {
            ResearchObservation::Fundamental(fact)
                if fact.concept().as_str() == "us-gaap:Assets" =>
            {
                assert_eq!(
                    fact.context().provenance().instrument_id(),
                    Some(instrument_id)
                );
                assert!(matches!(
                    fact.context().provenance().availability(),
                    AvailabilityEvidence::Unknown
                ));
                assert_eq!(
                    fact.context().time().effective().precision(),
                    ResearchTemporalPrecision::CalendarDate
                );
                Some(fact.context().time().revision().get())
            }
            _ => None,
        });
    assert_eq!(asset_revisions.next(), Some(1));
    assert_eq!(asset_revisions.next(), Some(2));
    assert_eq!(asset_revisions.next(), None);

    let submissions = RetrievedSubmissions::import_exact_bytes(
        include_bytes!("../fixtures/submissions-recent.json"),
        &[include_bytes!("../fixtures/submissions-archive.json").as_slice()],
        &store,
        SecParserLimits::production_defaults(),
    )?;
    let filing_ingested_at = submissions.raw().received_at().checked_add_nanos(1)?;
    let filings = normalize_filings(&source_id, &identities, &submissions, filing_ingested_at)?;
    assert_eq!(filings.len(), 3);
    let amendment = filings
        .iter()
        .find_map(|observation| match observation {
            ResearchObservation::Filing(filing)
                if filing.accession().as_str() == "0000320193-25-000080" =>
            {
                Some(filing)
            }
            _ => None,
        })
        .ok_or("missing canonical amendment")?;
    assert_eq!(amendment.context().time().revision().get(), 2);
    assert_eq!(
        amendment.context().time().effective().precision(),
        ResearchTemporalPrecision::CalendarDate
    );
    assert!(amendment.context().time().superseded().is_none());
    assert!(matches!(
        amendment.context().provenance().availability(),
        AvailabilityEvidence::Unknown
    ));
    Ok(())
}
