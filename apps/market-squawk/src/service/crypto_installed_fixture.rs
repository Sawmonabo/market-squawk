//! Debug-only read proof for an installed public crypto publication.
//!
//! The handle retains an immutable selector only. It has no publication writer, lease, or
//! activation capability, and reopening uses the newly installed service's research store.

use std::{
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use anyhow::{Context as _, bail};
use market_squawk_data::MarketEventCommitRef;
use market_squawk_domain::{EvidenceDigest, InstrumentId, LiveProvenance, MarketEvent, VenueId};
use tokio_util::sync::CancellationToken;

use super::InstalledService;
use crate::{
    ResearchService,
    application::{MarketEventRestartSelector, MarketRuntimeRegistry},
};

/// Weak read-only fixture access; it cannot keep either installed owner alive across restart.
#[derive(Clone, Debug)]
pub struct CryptoInstalledPublicationReader {
    market: Weak<MarketRuntimeRegistry>,
    research: Weak<ResearchService>,
}

/// Immutable exact publication coordinate captured from an installed, selected source route.
#[derive(Clone, Debug)]
pub struct CryptoInstalledPublicationProbe {
    selector: MarketEventRestartSelector,
    instrument_id: InstrumentId,
    venue_id: VenueId,
    connection_generation: u64,
    metadata_revision: String,
}

/// Typed events reopened from the exact original commit and native publication evidence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CryptoInstalledTypedRead {
    commit: MarketEventCommitRef,
    publication_digest: EvidenceDigest,
    source_id: String,
    events: Vec<MarketEvent>,
}

impl CryptoInstalledTypedRead {
    pub fn commit(&self) -> &MarketEventCommitRef {
        &self.commit
    }

    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.publication_digest
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn events(&self) -> &[MarketEvent] {
        &self.events
    }
}

impl InstalledService {
    /// Borrow the installed read path before `run` moves the service into its owner task.
    pub fn crypto_installed_publication_reader(&self) -> CryptoInstalledPublicationReader {
        CryptoInstalledPublicationReader {
            market: Arc::downgrade(&self.product.market_runtime()),
            research: Arc::downgrade(&self.product.research()),
        }
    }
}

impl CryptoInstalledPublicationReader {
    /// Reads an already-published receipt for the exact route selected by the public fixture.
    /// An absent receipt means the live publisher has not committed; it is never synthesized.
    pub async fn crypto_installed_publication_probe(
        &self,
        surface: &str,
        source_id: &str,
        instrument_id: InstrumentId,
        venue_id: &VenueId,
        connection_generation: u64,
    ) -> anyhow::Result<Option<CryptoInstalledPublicationProbe>> {
        if !matches!(
            surface,
            "coinbase.public-market-data" | "kraken.spot-public-market-data"
        ) {
            bail!("unsupported public crypto fixture surface");
        }
        if connection_generation == 0 {
            bail!("selected crypto connection generation was zero");
        }
        let cancellation = CancellationToken::new();
        let market = self
            .market
            .upgrade()
            .context("installed market owner has stopped")?;
        let routes = market
            .market_event_durable_route_reads(
                Instant::now() + Duration::from_secs(5),
                &cancellation,
            )
            .await
            .context("read installed crypto durable routes")?;
        let mut selected = None;
        for route in routes {
            if route.surface_id().as_str() != surface
                || route.metadata().source_id().as_str() != source_id
                || route.route().instrument() != instrument_id
                || route.route().venue() != venue_id
            {
                continue;
            }
            if selected.is_some() {
                bail!("ambiguous installed crypto durable route");
            }
            let Some(receipt) = route.read().latest_publication().await else {
                return Ok(None);
            };
            if receipt.restart_selector().source_id() != route.metadata().source_id()
                || receipt.event_count() == 0
            {
                bail!("installed crypto receipt did not match its selected route");
            }
            selected = Some(CryptoInstalledPublicationProbe {
                selector: receipt.restart_selector().clone(),
                instrument_id,
                venue_id: venue_id.clone(),
                connection_generation,
                metadata_revision: route
                    .metadata()
                    .revision()
                    .as_source_identifier()
                    .as_str()
                    .to_owned(),
            });
        }
        Ok(selected)
    }
}

impl CryptoInstalledPublicationProbe {
    pub fn commit(&self) -> &MarketEventCommitRef {
        self.selector.commit()
    }

    pub const fn publication_digest(&self) -> EvidenceDigest {
        self.selector.publication_digest()
    }

    pub fn source_id(&self) -> &str {
        self.selector.source_id().as_str()
    }

    /// Performs the production exact-restart read against a newly composed installed service.
    pub async fn reopen(
        &self,
        reader: &CryptoInstalledPublicationReader,
        deadline: Instant,
    ) -> anyhow::Result<CryptoInstalledTypedRead> {
        let research = reader
            .research
            .upgrade()
            .context("installed research owner has stopped")?;
        let reopened = self
            .selector
            .reopen(research.as_ref(), deadline, CancellationToken::new())
            .await
            .context("reopen exact installed crypto raw/native evidence and committed events")?;
        let events = reopened.events().events();
        if events.is_empty()
            || events.iter().any(|event| {
                let binding = event_provenance(event).binding();
                binding.source_id() != self.selector.source_id()
                    || binding.instrument_id() != Some(self.instrument_id)
                    || binding.venue_id() != &self.venue_id
                    || binding.connection_generation().get() != self.connection_generation
                    || binding.metadata_revision().as_source_identifier().as_str()
                        != self.metadata_revision
            })
        {
            bail!("reopened crypto events escaped their selected source, instrument, or venue");
        }
        Ok(CryptoInstalledTypedRead {
            commit: self.selector.commit().clone(),
            publication_digest: self.selector.publication_digest(),
            source_id: self.selector.source_id().as_str().to_owned(),
            events: events.to_vec(),
        })
    }
}

const fn event_provenance(event: &MarketEvent) -> &LiveProvenance {
    match event {
        MarketEvent::Trade(event) => event.provenance(),
        MarketEvent::Quote(event) => event.provenance(),
        MarketEvent::MarketDataQuote(event) => event.provenance(),
        MarketEvent::MarketDataTrade(event) => event.provenance(),
        MarketEvent::MarketDataBook(event) => event.provenance(),
        MarketEvent::MarketDataChart(event) => event.provenance(),
        MarketEvent::MarketDataScreener(event) => event.provenance(),
        MarketEvent::BookSnapshot(event) => event.provenance(),
        MarketEvent::BookDelta(event) => event.provenance(),
        MarketEvent::Auction(event) => event.provenance(),
        MarketEvent::TradingHalt(event) => event.provenance(),
        MarketEvent::InstrumentStatus(event) => event.provenance(),
        MarketEvent::CorporateAction(event) => event.provenance(),
    }
}
