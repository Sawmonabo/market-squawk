use std::error::Error;

use cap_std::{ambient_authority, fs::Dir};
use market_squawk_adapter_sec::{
    RawEvidenceStore, RetrievedCompanyFacts, RetrievedSubmissions, SecParserLimits,
    normalize_company_facts, normalize_filings,
};
use market_squawk_domain::{
    AvailabilityEvidence, EffectiveInterval, EvidenceDigest, FundamentalAmendmentStatus,
    FundamentalCadence, FundamentalConsolidation, FundamentalPeriod, FundamentalRestatementStatus,
    InstrumentId, MetadataRevision, PayloadHashAlgorithm, ProviderIdentityEvidence,
    ProviderIdentityRecord, ProviderIdentityRecordInput, ProviderIdentityRegistry,
    ProviderInstrumentId, ResearchObservation, ResearchTemporalCoordinate,
    ResearchTemporalPrecision, SourceId, SourceIdentifier, Timestamp,
};
use sha2::{Digest as _, Sha256};
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
                assert_eq!(
                    fact.context()
                        .time()
                        .published()
                        .map(ResearchTemporalCoordinate::precision),
                    Some(ResearchTemporalPrecision::CalendarDate)
                );
                let source = fact.fact_context();
                assert_eq!(source.unit().as_str(), "USD");
                assert!(matches!(source.period(), FundamentalPeriod::Instant { .. }));
                assert_eq!(source.period().end().to_string(), "2025-06-28");
                assert_eq!(source.fiscal_year(), Some(2025));
                assert_eq!(
                    source.fiscal_period().map(SourceIdentifier::as_str),
                    Some("Q3")
                );
                assert_eq!(source.cadence(), FundamentalCadence::Quarterly);
                assert!(source.dimensions().dimensions().is_none());
                assert_eq!(
                    source.consolidation(),
                    FundamentalConsolidation::Unavailable
                );
                assert!(matches!(
                    source.restatement_status(),
                    FundamentalRestatementStatus::Unavailable
                ));
                assert_eq!(
                    source.revision_order().ordinal(),
                    fact.context().time().revision()
                );
                if source.accession().as_str() == "0000320193-25-000080" {
                    assert_eq!(
                        source.amendment_status(),
                        FundamentalAmendmentStatus::Amendment
                    );
                    assert_eq!(
                        source.filing_form().map(SourceIdentifier::as_str),
                        Some("10-Q/A")
                    );
                    assert!(source.frame().is_none());
                } else {
                    assert_eq!(
                        source.amendment_status(),
                        FundamentalAmendmentStatus::Original
                    );
                    assert_eq!(
                        source.filing_form().map(SourceIdentifier::as_str),
                        Some("10-Q")
                    );
                    assert_eq!(
                        source.frame().map(SourceIdentifier::as_str),
                        Some("CY2025Q2I")
                    );
                }
                Some(fact.context().time().revision().get())
            }
            _ => None,
        });
    assert_eq!(asset_revisions.next(), Some(1));
    assert_eq!(asset_revisions.next(), Some(2));
    assert_eq!(asset_revisions.next(), None);

    let colliding_facts = br#"{
        "cik":"0000320193","entityName":"APPLE INC",
        "facts":{"us-gaap":{"Assets":{"units":{"USD":[
            {"end":"2025-06-28","val":331495000000,"accn":"0000320193-25-000079","fy":2025,"fp":"Q3","form":"10-Q","filed":"2025-07-31","frame":"CY2025Q2I"},
            {"end":"2025-06-28","val":331495000000,"accn":"0000320193-25-000079","fy":2025,"fp":"Q3","form":"10-Q","filed":"2025-07-31","frame":"CY2025Q2I"}
        ]}}}}}
    }"#;
    let colliding = RetrievedCompanyFacts::import_exact_bytes(
        colliding_facts,
        &store,
        SecParserLimits::production_defaults(),
    )?;
    let colliding_ingested_at = colliding.raw().received_at().checked_add_nanos(1)?;
    let canonical =
        normalize_company_facts(&source_id, &identities, &colliding, colliding_ingested_at)?;
    let canonical_ids: Vec<_> = canonical
        .iter()
        .filter_map(|observation| match observation {
            ResearchObservation::Fundamental(fact) => {
                Some(fact.context().provenance().source_identifier().as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(canonical_ids.len(), 2);
    assert_ne!(canonical_ids[0], canonical_ids[1]);
    assert!(canonical_ids[0].ends_with(":0"));
    assert!(canonical_ids[1].ends_with(":1"));
    let native_rows = colliding.document().occurrences();
    assert_eq!(native_rows[0].source_ordinal(), 0);
    assert_eq!(native_rows[1].source_ordinal(), 1);
    let native_ids = [
        Sha256::digest(serde_json::to_vec(&native_rows[0])?),
        Sha256::digest(serde_json::to_vec(&native_rows[1])?),
    ];
    assert_ne!(native_ids[0], native_ids[1]);

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
    assert!(amendment.context().time().published().is_some());
    assert!(amendment.context().time().superseded().is_none());
    assert!(matches!(
        amendment.context().provenance().availability(),
        AvailabilityEvidence::Unknown
    ));
    Ok(())
}
