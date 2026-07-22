mod classification;
mod workflow;

use std::error::Error;
use std::time::Duration;

use market_squawk_data::{
    CatalogAuthority, CatalogConfig, CatalogLimit, CatalogResultLimits, DatasetId,
    DatasetManifestRef, DatasetSchemaRegistry, Sha256Digest,
};
use market_squawk_domain::{
    AccountId, Currency, InstrumentId, Money, RevisionNumber, SourceIdentifier, Timestamp,
};
use market_squawk_platform::LocalPaths;
use market_squawk_portfolio::{
    CashFlow, CashFlowKind, LedgerEntry, LedgerEntryKind, LotSelection, PortfolioLedger,
    PortfolioLimitInput, PortfolioLimits, PortfolioRevision, PriceEvidence, RevisionEvidence,
    Trade, TradeSide, TransactionRevision, ValuationSet,
};
use market_squawk_valuation::{
    ActorId, FairValueLimitInput, FairValueLimits, FairValueService, InputSignificance,
    ValuationInput, ValuationMeasurement, ValuationMeasurementSpec, ValuationMethod,
};
use rust_decimal::Decimal;

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

const MEASUREMENT_AT: i64 = 1_000;
const PREPARED_AT: i64 = 1_200;

fn account() -> Result<AccountId, Box<dyn Error>> {
    Ok("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa".parse()?)
}

fn instrument() -> Result<InstrumentId, Box<dyn Error>> {
    Ok("9f3914d3-9ef4-42f7-a707-3f2dcde861d1".parse()?)
}

fn actor(value: &str) -> Result<ActorId, Box<dyn Error>> {
    Ok(ActorId::try_from(value)?)
}

fn source(value: &str) -> Result<SourceIdentifier, Box<dyn Error>> {
    Ok(SourceIdentifier::try_from(value)?)
}

fn portfolio_limits() -> Result<PortfolioLimits, Box<dyn Error>> {
    Ok(PortfolioLimits::try_new(PortfolioLimitInput {
        max_accounts: 1,
        max_instruments: 2,
        max_lots: 4,
        max_transactions: 4,
        max_factors: 1,
        max_scenarios: 1,
        max_history: 2,
        max_results: 4,
        max_retained_bytes: 256 * 1024,
    })?)
}

fn dataset(marker: u8) -> Result<DatasetManifestRef, Box<dyn Error>> {
    Ok(DatasetManifestRef::try_new_with_schema(
        DatasetId::try_from("fair-value-test")?,
        u64::from(marker),
        DatasetSchemaRegistry::local().canonical_research_observations()?,
        Sha256Digest::new([marker; 32]),
    )?)
}

fn portfolio_revision(as_of: i64, marker: u8) -> Result<PortfolioRevision, Box<dyn Error>> {
    let account = account()?;
    let instrument = instrument()?;
    let currency = Currency::try_from("USD")?;
    let limits = portfolio_limits()?;
    let manifest = dataset(marker)?;
    let mut ledger = PortfolioLedger::try_new(account, currency, limits)?;
    let entries = vec![
        LedgerEntry::try_new(
            account,
            TransactionRevision::try_new(source("deposit")?, RevisionNumber::new(1)?, None)?,
            Timestamp::from_unix_nanos(1),
            source("cash-source")?,
            LedgerEntryKind::CashFlow(CashFlow::try_new(
                CashFlowKind::Deposit,
                Money::new(Decimal::from(1_000), currency),
                None,
            )?),
        )?,
        LedgerEntry::try_new(
            account,
            TransactionRevision::try_new(source("buy")?, RevisionNumber::new(1)?, None)?,
            Timestamp::from_unix_nanos(2),
            source("trade-source")?,
            LedgerEntryKind::Trade(Trade::try_new(
                TradeSide::Buy,
                instrument,
                Decimal::TEN,
                Money::new(Decimal::TEN, currency),
                Money::new(Decimal::ZERO, currency),
                LotSelection::Fifo,
            )?),
        )?,
    ];
    let valuation = ValuationSet::try_new(
        currency,
        Timestamp::from_unix_nanos(as_of),
        manifest.clone(),
        Sha256Digest::new([marker; 32]),
        vec![PriceEvidence::try_new(
            instrument,
            Money::new(Decimal::TEN, currency),
            Timestamp::from_unix_nanos(as_of),
            source("price-source")?,
        )?],
        Vec::new(),
        limits,
    )?;
    let evidence = RevisionEvidence::try_new(
        Timestamp::from_unix_nanos(as_of),
        manifest,
        Sha256Digest::new([marker; 32]),
        Sha256Digest::new([marker.wrapping_add(1); 32]),
        vec![source("ledger-source")?],
        Vec::new(),
        None,
    )?;
    Ok(ledger.try_apply(entries, None, valuation, evidence)?)
}

fn measurement(as_of: i64, marker: u8) -> Result<ValuationMeasurement, Box<dyn Error>> {
    let revision = portfolio_revision(as_of, marker)?;
    let input = ValuationInput::from_portfolio_position(
        &revision,
        instrument()?,
        InputSignificance::Significant,
    )?;
    Ok(ValuationMeasurement::try_new(ValuationMeasurementSpec {
        account_id: account()?,
        instrument_id: instrument()?,
        amount: input.amount(),
        measurement_at: Timestamp::from_unix_nanos(MEASUREMENT_AT),
        prepared_at: Timestamp::from_unix_nanos(PREPARED_AT),
        prepared_by: actor("preparer")?,
        method: ValuationMethod::MarketApproach,
        inputs: vec![input],
    })?)
}

fn fair_value_limits(max_query_results: usize) -> Result<FairValueLimits, Box<dyn Error>> {
    Ok(FairValueLimits::try_new(FairValueLimitInput {
        max_measurements: 16,
        max_inputs_per_measurement: 4,
        max_records_per_family: 32,
        max_query_results,
        max_retained_bytes: 2 * 1024 * 1024,
    })?)
}

struct CatalogFixture {
    _directory: tempfile::TempDir,
    authority: CatalogAuthority,
}

impl CatalogFixture {
    fn open() -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::tempdir()?;
        let paths = LocalPaths::prepare(directory.path().join("local"))?;
        let config = CatalogConfig::try_new(
            paths.catalog()?.clone(),
            Duration::from_millis(750),
            CatalogLimit::new(32)?,
            CatalogResultLimits::try_new(1024 * 1024, 16 * 1024 * 1024)?,
        )?;
        Ok(Self {
            _directory: directory,
            authority: CatalogAuthority::open(config)?,
        })
    }

    fn service(&self, max_query_results: usize) -> Result<FairValueService<'_>, Box<dyn Error>> {
        Ok(FairValueService::open(
            &self.authority,
            fair_value_limits(max_query_results)?,
        )?)
    }
}
