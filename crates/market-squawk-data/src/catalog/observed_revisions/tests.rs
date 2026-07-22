use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use market_squawk_domain::{
    CalendarDate, ResearchTemporalCoordinate, SourceId, SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use market_squawk_sources::{
    CanonicalObservationFamily, ObservedProviderOrder, ObservedRevisionAuthority,
    ObservedRevisionBatch, ObservedRevisionError, ObservedRevisionRecord, ObservedSemanticPayload,
    ObservedVersionEvidence,
};
use rusqlite::params;
use tokio_util::sync::CancellationToken;

use super::super::{CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits};
use super::CatalogObservedRevisionAuthority;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn durable_assignment_is_ordered_idempotent_atomic_and_restart_stable()
-> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let paths = LocalPaths::prepare(directory.path().join("state"))?;
    let location = paths.catalog()?.clone();
    let config = CatalogConfig::try_new(
        location,
        Duration::from_millis(750),
        CatalogLimit::new(32)?,
        CatalogResultLimits::try_new(1024 * 1024, 8 * 1024 * 1024)?,
    )?;
    let source = SourceId::try_from("fred-observed-fixture")?;
    let gdp_family = family(&source, "GDP")?;
    let first = provider_record(&gdp_family, b"v1", b"payload-v1", 1)?;
    let second = provider_record(&gdp_family, b"v2", b"payload-v2", 2)?;
    let local_family = family(&source, "LOCAL")?;

    let catalog = CatalogAuthority::open(config.clone())?;
    register_source_fixture(&catalog, &source)?;
    let shared = Arc::new(Mutex::new(catalog));
    let authority = revision_authority(&shared);
    let assignments = authority
        .assign(
            ObservedRevisionBatch::try_new(
                source.clone(),
                vec![second.clone(), first.clone(), first.clone()],
            )?,
            Instant::now() + Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        assignments
            .as_slice()
            .iter()
            .map(|revision| revision.get())
            .collect::<Vec<_>>(),
        vec![2, 1, 1]
    );
    let replay = authority
        .assign(
            ObservedRevisionBatch::try_new(source.clone(), vec![second.clone(), first.clone()])?,
            Instant::now() + Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await?;
    assert_eq!(
        replay
            .as_slice()
            .iter()
            .map(|revision| revision.get())
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    authority
        .assign(
            ObservedRevisionBatch::try_new(
                source.clone(),
                vec![local_record(&local_family, b"local-v1")?],
            )?,
            Instant::now() + Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await?;
    drop(authority);
    drop(shared);

    let shared = Arc::new(Mutex::new(CatalogAuthority::open(config)?));
    let authority = revision_authority(&shared);
    authority
        .assign(
            ObservedRevisionBatch::try_new(
                source.clone(),
                vec![local_record(&local_family, b"local-v2")?],
            )?,
            Instant::now() + Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await?;
    {
        let catalog = shared.lock().map_err(|_| "catalog mutex poisoned")?;
        let history = catalog
            .catalog()
            .observed_revision_history(&gdp_family, CatalogLimit::new(8)?)?;
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].revision().get(), 1);
        assert_eq!(history[1].revision().get(), 2);
        assert_eq!(history[0].version().exact_evidence(), b"v1");
        assert_eq!(
            history[1].semantic_payload().exact_evidence(),
            b"payload-v2"
        );
        assert_eq!(
            history[1]
                .provider_order()
                .ok_or("missing retained provider order")?
                .coordinate(),
            &ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2026, 1, 2)?)
        );
        let local_history = catalog
            .catalog()
            .observed_revision_history(&local_family, CatalogLimit::new(8)?)?;
        assert_eq!(
            local_history
                .iter()
                .map(|revision| revision.revision().get())
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }
    assert_eq!(
        authority
            .assign(
                ObservedRevisionBatch::try_new(
                    source.clone(),
                    vec![provider_record(&local_family, b"provider", b"provider", 4,)?],
                )?,
                Instant::now() + Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await,
        Err(ObservedRevisionError::Conflict)
    );

    let conflicting = provider_record(&gdp_family, b"v1", b"changed-payload", 1)?;
    assert_eq!(
        authority
            .assign(
                ObservedRevisionBatch::try_new(source.clone(), vec![conflicting])?,
                Instant::now() + Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await,
        Err(ObservedRevisionError::Conflict)
    );
    let predecessor = provider_record(&gdp_family, b"v0", b"payload-v0", 1)?;
    assert_eq!(
        authority
            .assign(
                ObservedRevisionBatch::try_new(source.clone(), vec![predecessor])?,
                Instant::now() + Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await,
        Err(ObservedRevisionError::BackfillRejected)
    );

    let earlier_family = family(&source, "AAA")?;
    let later_family = family(&source, "ZZZ")?;
    let retained_later = provider_record(&later_family, b"stable", b"retained", 3)?;
    authority
        .assign(
            ObservedRevisionBatch::try_new(source.clone(), vec![retained_later])?,
            Instant::now() + Duration::from_secs(5),
            CancellationToken::new(),
        )
        .await?;
    let new_earlier = provider_record(&earlier_family, b"new", b"new", 3)?;
    let conflict_later = provider_record(&later_family, b"stable", b"divergent", 3)?;
    assert_eq!(
        authority
            .assign(
                ObservedRevisionBatch::try_new(source, vec![new_earlier, conflict_later])?,
                Instant::now() + Duration::from_secs(5),
                CancellationToken::new(),
            )
            .await,
        Err(ObservedRevisionError::Conflict)
    );
    let catalog = shared.lock().map_err(|_| "catalog mutex poisoned")?;
    assert!(
        catalog
            .catalog()
            .observed_revision_history(&earlier_family, CatalogLimit::new(8)?)?
            .is_empty()
    );
    assert_eq!(
        catalog
            .catalog()
            .observed_revision_history(&gdp_family, CatalogLimit::new(8)?)?
            .len(),
        2
    );
    Ok(())
}

fn revision_authority(shared: &Arc<Mutex<CatalogAuthority>>) -> Arc<dyn ObservedRevisionAuthority> {
    Arc::new(CatalogObservedRevisionAuthority::new(Arc::clone(shared)))
}

fn family(source: &SourceId, series: &str) -> Result<CanonicalObservationFamily, Box<dyn Error>> {
    Ok(CanonicalObservationFamily::macro_observation(
        source,
        &SourceIdentifier::try_from(series)?,
        &ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2026, 1, 1)?),
    )?)
}

fn provider_record(
    family: &CanonicalObservationFamily,
    version: &[u8],
    payload: &[u8],
    day: u8,
) -> Result<ObservedRevisionRecord, Box<dyn Error>> {
    Ok(ObservedRevisionRecord::try_new(
        family.clone(),
        ObservedVersionEvidence::provider_supplied(version)?,
        ObservedSemanticPayload::try_from_bytes(payload)?,
        Some(ObservedProviderOrder::try_new(
            ResearchTemporalCoordinate::calendar_date(CalendarDate::new(2026, 1, day)?),
            version,
        )?),
    )?)
}

fn local_record(
    family: &CanonicalObservationFamily,
    payload: &[u8],
) -> Result<ObservedRevisionRecord, Box<dyn Error>> {
    let payload = ObservedSemanticPayload::try_from_bytes(payload)?;
    Ok(ObservedRevisionRecord::try_new(
        family.clone(),
        ObservedVersionEvidence::locally_observed_content(&payload)?,
        payload,
        None,
    )?)
}

fn register_source_fixture(
    authority: &CatalogAuthority,
    source: &SourceId,
) -> Result<(), Box<dyn Error>> {
    let transaction = authority.catalog().connection.unchecked_transaction()?;
    let revision_digest = [7_u8; 32];
    transaction.execute(
        "INSERT INTO sources
         (source_id, current_revision_digest, current_registered_at_ns, first_registered_at_ns)
         VALUES (?1, ?2, 1, 1)",
        params![source.as_str(), revision_digest],
    )?;
    transaction.execute(
        "INSERT INTO source_revisions
         (source_id, revision_digest, metadata_json, registered_at_ns)
         VALUES (?1, ?2, '{}', ?3)",
        params![
            source.as_str(),
            revision_digest,
            Timestamp::from_unix_nanos(1).unix_nanos()
        ],
    )?;
    transaction.commit()?;
    Ok(())
}
