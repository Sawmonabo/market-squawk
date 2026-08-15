use market_squawk_adapter_treasury::{
    AverageInterestRate, DailyParYieldCurvePage, FiscalDataPage, FiscalDataParseLimits,
    TreasuryAuthorizedUse, TreasuryBillMaturity, TreasuryBillRateMeasure, TreasuryDailyRateFamily,
    TreasuryDailyRateMetric, TreasuryDailyRatePage, TreasuryDailyRatePaginationTracker,
    TreasuryDailyRateQuery, TreasuryDailyRatesConfig, TreasuryDashboardReadPlan,
    TreasuryDatasetFamily, TreasuryDatasetPeriod, TreasuryExtrapolationFactor, TreasuryFiscalQuery,
    TreasuryLongTermRateType, TreasuryOwnerUseAttestation, TreasuryPublicationMode,
    TreasuryRateProfile, TreasurySourceConfig, TreasurySurface, TreasuryYieldCurveProfile,
};
use market_squawk_domain::{CalendarDate, DataQuality, DigestAlgorithm, EvidenceDigest, Timestamp};
use sha2::{Digest, Sha256};
use std::num::NonZeroU16;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn owner_use_attestation(
    at: Timestamp,
) -> Result<TreasuryOwnerUseAttestation, Box<dyn std::error::Error>> {
    Ok(TreasuryOwnerUseAttestation::try_private_personal_research(
        EvidenceDigest::new(
            DigestAlgorithm::Sha256,
            Sha256::digest(b"owner-authorized-private-treasury-research").into(),
        ),
        at,
    )?)
}

#[test]
fn official_average_rate_profile_preserves_exact_decimal_and_methodology_evidence() -> TestResult {
    let query = TreasuryFiscalQuery::average_interest_rates_v2(
        CalendarDate::new(2026, 1, 1)?,
        CalendarDate::new(2026, 12, 31)?,
        NonZeroU16::new(1).ok_or("page size must be non-zero")?,
    )?;
    let page = FiscalDataPage::parse(
        include_bytes!("../fixtures/average_interest_rates.json"),
        &query.page(1)?,
        FiscalDataParseLimits::production_defaults(),
    )?;
    let profile = TreasuryRateProfile::average_interest_rates_v2();
    let rate = AverageInterestRate::try_from_record(&page.records()[0], &profile)?;
    assert_eq!(rate.record_date().to_string(), "2026-06-30");
    assert_eq!(rate.rate_percent().to_string(), "3.706");
    assert_eq!(rate.security_description(), "Treasury Bills");
    assert_eq!(rate.source_line_number(), "1");
    assert_eq!(rate.source_payload_digest(), page.response_payload_digest());
    assert_eq!(profile.endpoint(), "/v2/accounting/od/avg_interest_rates");
    assert!(
        profile
            .source_url()
            .starts_with("https://fiscaldata.treasury.gov/")
    );
    assert_eq!(rate.schema_digest(), page.schema_digest());
    Ok(())
}

#[test]
fn daily_par_yield_curve_is_civil_dated_and_official_delayed() -> TestResult {
    let profile = TreasuryYieldCurveProfile::daily_par_yield_curve();
    let request = profile.page(2026, 0)?;
    assert!(!request.url().contains("page="));
    assert!(profile.page(2026, 1).is_err());
    let exact_payload = include_bytes!("../fixtures/daily_par_yield_curve.xml");
    let page = DailyParYieldCurvePage::parse(
        exact_payload,
        &request,
        FiscalDataParseLimits::production_defaults(),
    )?;

    let observation = &page.observations()[0];
    assert_eq!(profile.quality(), DataQuality::OfficialDelayed);
    assert_eq!(observation.record_date().to_string(), "2026-01-02");
    assert_eq!(observation.source_record_id(), "140");
    assert_eq!(
        observation
            .one_month_percent()
            .map(|value| value.to_string())
            .as_deref(),
        Some("3.72")
    );
    assert_eq!(
        observation
            .thirty_year_percent()
            .map(|value| value.to_string())
            .as_deref(),
        Some("4.86")
    );
    assert_eq!(
        observation.source_payload_digest(),
        page.response_payload_digest()
    );
    assert_eq!(
        page.response_payload_digest(),
        <[u8; 32]>::from(Sha256::digest(exact_payload))
    );
    let payload_without_redundant_ids = std::str::from_utf8(exact_payload)?
        .replace(
            "    <id>https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_yield_curve&amp;id=140</id>\n",
            "",
        )
        .replace("        <d:Id m:type=\"Edm.Int32\">140</d:Id>\n", "");
    let page_without_redundant_ids = DailyParYieldCurvePage::parse(
        payload_without_redundant_ids.as_bytes(),
        &request,
        FiscalDataParseLimits::production_defaults(),
    )?;
    assert_eq!(
        page_without_redundant_ids.observations()[0].source_record_id(),
        "date:2026-01-02"
    );
    assert!(
        profile
            .methodology_url()
            .starts_with("https://home.treasury.gov/")
    );
    Ok(())
}

#[test]
fn daily_par_yield_curve_rejects_wrong_namespace_and_rows_without_rates() -> TestResult {
    let profile = TreasuryYieldCurveProfile::daily_par_yield_curve();
    let request = profile.page(2026, 0)?;
    let exact_payload =
        std::str::from_utf8(include_bytes!("../fixtures/daily_par_yield_curve.xml"))?;
    let wrong_namespace = exact_payload.replace(
        "http://schemas.microsoft.com/ado/2007/08/dataservices\"",
        "https://attacker.invalid/dataservices\"",
    );
    assert!(
        DailyParYieldCurvePage::parse(
            wrong_namespace.as_bytes(),
            &request,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );

    for invalid in [
        "not-an-rfc3339-instant",
        "2026-07-21T06:54:08z",
        "2026-07-21T06:54:08-00:00",
    ] {
        let invalid_atom_date = exact_payload.replace("2026-07-21T06:54:08Z", invalid);
        assert!(
            DailyParYieldCurvePage::parse(
                invalid_atom_date.as_bytes(),
                &request,
                FiscalDataParseLimits::production_defaults(),
            )
            .is_err()
        );
    }

    let no_rates = br#"<?xml version="1.0"?>
      <feed xmlns="http://www.w3.org/2005/Atom"
            xmlns:d="http://schemas.microsoft.com/ado/2007/08/dataservices"
            xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
        <title>DailyTreasuryYieldCurveRateData</title>
        <id>https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_yield_curve</id>
        <updated>2026-07-21T06:54:08Z</updated>
        <entry>
          <id>https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml-item?data=daily_treasury_yield_curve&amp;id=140</id>
          <updated>2026-07-21T06:54:08Z</updated>
          <content><m:properties>
            <d:Id m:type="Edm.Int32">140</d:Id>
            <d:NEW_DATE m:type="Edm.DateTime">2026-01-02T00:00:00</d:NEW_DATE>
          </m:properties></content>
        </entry>
      </feed>"#;
    assert!(
        DailyParYieldCurvePage::parse(
            no_rates,
            &request,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn daily_rate_queries_bind_all_five_official_datasets_and_periods() -> TestResult {
    let families = [
        (
            TreasuryDailyRateFamily::NominalParYieldCurve,
            "daily_treasury_yield_curve",
            1990,
            "DailyTreasuryYieldCurveRateData",
            "daily-par-yield-curve",
        ),
        (
            TreasuryDailyRateFamily::BillRates,
            "daily_treasury_bill_rates",
            2002,
            "DailyTreasuryBillRateData",
            "daily-bill-rates",
        ),
        (
            TreasuryDailyRateFamily::LongTermRates,
            "daily_treasury_long_term_rate",
            2000,
            "DailyTreasuryLongTermRateData",
            "daily-long-term-rates",
        ),
        (
            TreasuryDailyRateFamily::RealParYieldCurve,
            "daily_treasury_real_yield_curve",
            2003,
            "DailyTreasuryRealYieldCurveRateData",
            "daily-real-par-yield-curve",
        ),
        (
            TreasuryDailyRateFamily::RealLongTermRates,
            "daily_treasury_real_long_term",
            2000,
            "DailyTreasuryRealLongTermRateAverageData",
            "daily-real-long-term-rates",
        ),
    ];
    assert_eq!(
        TreasuryDailyRateFamily::ALL,
        families.map(|(family, _, _, _, _)| family)
    );

    for (family, provider_key, start_year, feed_title, dataset_token) in families {
        assert_eq!(family.provider_key(), provider_key);
        assert_eq!(family.start_year(), start_year);
        assert_eq!(family.feed_title(), feed_title);
        assert_eq!(family.dataset_family_token(), dataset_token);
        assert_eq!(family.quality(), DataQuality::OfficialDelayed);
        assert!(family.feed_identity().ends_with(provider_key));

        let year = TreasuryDailyRateQuery::year(family, 2025)?;
        assert_eq!(
            year.dataset().as_str(),
            format!("treasury:{dataset_token}:2025")
        );
        assert_eq!(
            year.analytical_dataset().as_str(),
            format!("treasury.{dataset_token}.2025")
        );
        let year_page = year.page(0)?;
        assert!(year_page.url().contains(&format!("data={provider_key}")));
        assert!(year_page.url().contains("field_tdr_date_value=2025"));
        assert!(!year_page.url().contains("page="));
        assert!(year.page(1).is_err());

        let month = TreasuryDailyRateQuery::month(family, 2026, 1)?;
        assert_eq!(
            month.dataset().as_str(),
            format!("treasury:{dataset_token}:2026-01")
        );
        assert_eq!(
            month.analytical_dataset().as_str(),
            format!("treasury.{dataset_token}.2026-01")
        );
        assert!(
            month
                .page(0)?
                .url()
                .contains("field_tdr_date_value_month=202601")
        );
        assert!(month.page(1).is_err());

        let all = TreasuryDailyRateQuery::all_history(family)?;
        assert_eq!(
            all.dataset().as_str(),
            format!("treasury:{dataset_token}:all")
        );
        assert_eq!(
            all.analytical_dataset().as_str(),
            format!("treasury.{dataset_token}.all")
        );
        assert!(
            all.page(0)?
                .url()
                .contains("field_tdr_date_value=all&page=0")
        );
        assert!(
            all.page(7)?
                .url()
                .contains("field_tdr_date_value=all&page=7")
        );
        assert_ne!(year.query_digest(), month.query_digest());
        assert_ne!(all.page(0)?.request_digest(), all.page(1)?.request_digest());
        assert!(TreasuryDailyRateQuery::year(family, start_year - 1).is_err());
    }

    let config = TreasurySourceConfig::daily_rates_all_families(2025, 2025)?;
    let catalog = config.dataset_catalog()?;
    assert_eq!(catalog.surface(), TreasurySurface::DailyRatesXml);
    assert_eq!(catalog.datasets().len(), TreasuryDailyRateFamily::ALL.len());
    assert!(catalog.complete_selected_family_coverage());
    for family in TreasuryDailyRateFamily::ALL {
        let datasets = catalog
            .datasets()
            .iter()
            .filter(|dataset| dataset.family() == TreasuryDatasetFamily::DailyRate(family))
            .collect::<Vec<_>>();
        assert_eq!(datasets.len(), 1);
        assert_eq!(
            datasets[0].publication_mode(),
            TreasuryPublicationMode::AtomicResponse
        );
        assert!(!datasets[0].analytical_dataset().as_str().contains(':'));
    }
    let owner_use = owner_use_attestation(Timestamp::from_unix_nanos(1_800_000_000_000_000_000))?;
    assert!(owner_use.permits(TreasuryAuthorizedUse::TrainModel));
    assert!(owner_use.permits(TreasuryAuthorizedUse::OperateModel));
    assert!(owner_use.commercial_sale_prohibited());
    assert!(owner_use.redistribution_prohibited());
    let activation = config.activation_intent(owner_use)?;
    assert_eq!(activation.catalog(), &catalog);
    let read_plan = TreasuryDashboardReadPlan::try_new(&activation)?;
    assert!(read_plan.complete_selected_family_coverage());
    assert_eq!(
        read_plan.datasets().len(),
        TreasuryDailyRateFamily::ALL.len()
    );
    assert_eq!(
        read_plan
            .datasets()
            .iter()
            .map(|dataset| dataset.expected_series().len())
            .collect::<Vec<_>>(),
        [14, 28, 3, 5, 1]
    );
    let pre_additive = TreasurySourceConfig::daily_rates_all_families(2024, 2024)?;
    let pre_additive_plan = TreasuryDashboardReadPlan::try_new(&pre_additive.activation_intent(
        owner_use_attestation(Timestamp::from_unix_nanos(1_800_000_000_000_000_000))?,
    )?)?;
    assert_eq!(
        pre_additive_plan
            .datasets()
            .iter()
            .map(|dataset| dataset.expected_series().len())
            .collect::<Vec<_>>(),
        [13, 24, 3, 5, 1]
    );
    let multi_year = TreasurySourceConfig::daily_rates_all_families(2025, 2026)?;
    let multi_year_activation = multi_year.activation_intent(owner_use_attestation(
        Timestamp::from_unix_nanos(1_800_000_000_000_000_000),
    )?)?;
    let multi_year_dashboard = TreasuryDashboardReadPlan::try_new(&multi_year_activation)?;
    assert_eq!(
        multi_year_dashboard.datasets().len(),
        TreasuryDailyRateFamily::ALL.len()
    );
    assert!(multi_year_dashboard.datasets().iter().all(|dataset| {
        dataset.descriptor().period() == (TreasuryDatasetPeriod::CalendarYear { year: 2026 })
    }));
    let doctor = activation.doctor_plan(&config)?;
    assert_eq!(doctor.surface(), TreasurySurface::DailyRatesXml);
    assert_eq!(doctor.probes().len(), TreasuryDailyRateFamily::ALL.len());
    assert!(doctor.complete_selected_family_coverage());

    let all_history_config =
        TreasurySourceConfig::daily_rates(TreasuryDailyRatesConfig::try_new([
            TreasuryDailyRateQuery::all_history(TreasuryDailyRateFamily::NominalParYieldCurve)?,
        ])?);
    let all_history = all_history_config.dataset_catalog()?;
    assert_eq!(all_history.datasets().len(), 1);
    assert_eq!(
        all_history.datasets()[0].publication_mode(),
        TreasuryPublicationMode::ResumableBackfill
    );
    assert!(!all_history.complete_selected_family_coverage());
    let all_history_doctor = all_history_config.doctor_plan(owner_use_attestation(
        Timestamp::from_unix_nanos(1_800_000_000_000_000_000),
    )?)?;
    assert_eq!(all_history_doctor.probes().len(), 1);
    let all_history_families = TreasurySourceConfig::daily_rates_all_history()?;
    let all_history_activation = all_history_families.activation_intent(owner_use_attestation(
        Timestamp::from_unix_nanos(1_800_000_000_000_000_000),
    )?)?;
    assert_eq!(
        all_history_activation.catalog().datasets().len(),
        TreasuryDailyRateFamily::ALL.len()
    );
    assert!(
        all_history_activation
            .catalog()
            .datasets()
            .iter()
            .all(|dataset| dataset.publication_mode() == TreasuryPublicationMode::ResumableBackfill)
    );
    assert!(
        all_history_activation
            .catalog()
            .complete_selected_family_coverage()
    );
    assert_eq!(
        TreasuryDashboardReadPlan::try_new(&all_history_activation)?
            .datasets()
            .len(),
        TreasuryDailyRateFamily::ALL.len()
    );

    let ranked_doctor = TreasurySourceConfig::daily_rates(TreasuryDailyRatesConfig::try_new([
        TreasuryDailyRateQuery::all_history(TreasuryDailyRateFamily::NominalParYieldCurve)?,
        TreasuryDailyRateQuery::month(TreasuryDailyRateFamily::NominalParYieldCurve, 2024, 12)?,
        TreasuryDailyRateQuery::year(TreasuryDailyRateFamily::NominalParYieldCurve, 2025)?,
    ])?)
    .doctor_plan(owner_use_attestation(Timestamp::from_unix_nanos(
        1_800_000_000_000_000_000,
    ))?)?;
    assert_eq!(ranked_doctor.probes().len(), 1);
    assert_eq!(
        ranked_doctor.probes()[0].descriptor().period(),
        TreasuryDatasetPeriod::CalendarYear { year: 2025 }
    );
    Ok(())
}

#[test]
fn five_daily_rate_schemas_preserve_typed_rates_and_provider_metadata() -> TestResult {
    let limits = FiscalDataParseLimits::production_defaults();

    let nominal = TreasuryDailyRatePage::parse(
        include_bytes!("../fixtures/daily_par_yield_curve.xml"),
        &TreasuryDailyRateQuery::year(TreasuryDailyRateFamily::NominalParYieldCurve, 2026)?
            .page(0)?,
        limits,
    )?;
    assert_eq!(
        nominal.observations()[0]
            .point(TreasuryDailyRateMetric::NominalParYield(
                market_squawk_adapter_treasury::TreasuryMaturity::ThirtyYears,
            ))
            .map(|point| point.rate_percent().to_string())
            .as_deref(),
        Some("4.86")
    );

    let bills = TreasuryDailyRatePage::parse(
        include_bytes!("../fixtures/daily_bill_rates.xml"),
        &TreasuryDailyRateQuery::year(TreasuryDailyRateFamily::BillRates, 2026)?.page(0)?,
        limits,
    )?;
    let bill = bills.observations()[0]
        .point(TreasuryDailyRateMetric::Bill {
            maturity: TreasuryBillMaturity::FourWeeks,
            measure: TreasuryBillRateMeasure::BankDiscount,
        })
        .ok_or("missing four-week bill discount rate")?;
    assert_eq!(bill.rate_percent().to_string(), "3.58");
    assert_eq!(
        bill.maturity_date().map(|date| date.to_string()).as_deref(),
        Some("2026-02-03")
    );
    assert_eq!(bill.cusip(), Some("912797SJ7"));
    assert_eq!(
        bills.observations()[0]
            .point(TreasuryDailyRateMetric::Bill {
                maturity: TreasuryBillMaturity::FourWeeks,
                measure: TreasuryBillRateMeasure::CouponEquivalent,
            })
            .map(|point| point.rate_percent().to_string())
            .as_deref(),
        Some("3.64")
    );

    let long_term = TreasuryDailyRatePage::parse(
        include_bytes!("../fixtures/daily_long_term_rates.xml"),
        &TreasuryDailyRateQuery::year(TreasuryDailyRateFamily::LongTermRates, 2026)?.page(0)?,
        limits,
    )?;
    assert_eq!(long_term.observations().len(), 3);
    let real_rate = long_term
        .observations()
        .iter()
        .find_map(|observation| {
            observation.point(TreasuryDailyRateMetric::LongTerm(
                TreasuryLongTermRateType::RealRate,
            ))
        })
        .ok_or("missing typed long-term real rate")?;
    assert_eq!(real_rate.rate_percent().to_string(), "2.55");
    assert_eq!(
        real_rate.extrapolation_factor(),
        Some(TreasuryExtrapolationFactor::NotApplicable)
    );
    let long_term_history =
        TreasuryDailyRateQuery::all_history(TreasuryDailyRateFamily::LongTermRates)?;
    let long_term_history_page = TreasuryDailyRatePage::parse(
        include_bytes!("../fixtures/daily_long_term_rates.xml"),
        &long_term_history.page(0)?,
        limits,
    )?;
    let mut long_term_tracker =
        TreasuryDailyRatePaginationTracker::try_new(&long_term_history, 2, 600)?;
    assert!(!long_term_tracker.accept(&long_term_history_page)?);

    let real_curve = TreasuryDailyRatePage::parse(
        include_bytes!("../fixtures/daily_real_par_yield_curve.xml"),
        &TreasuryDailyRateQuery::year(TreasuryDailyRateFamily::RealParYieldCurve, 2026)?.page(0)?,
        limits,
    )?;
    assert_eq!(
        real_curve.observations()[0]
            .point(TreasuryDailyRateMetric::RealParYield(
                market_squawk_adapter_treasury::TreasuryMaturity::FiveYears,
            ))
            .map(|point| point.rate_percent().to_string())
            .as_deref(),
        Some("1.46")
    );

    let real_long_term = TreasuryDailyRatePage::parse(
        include_bytes!("../fixtures/daily_real_long_term_rates.xml"),
        &TreasuryDailyRateQuery::year(TreasuryDailyRateFamily::RealLongTermRates, 2026)?.page(0)?,
        limits,
    )?;
    assert_eq!(
        real_long_term.observations()[0]
            .point(TreasuryDailyRateMetric::RealLongTermAverage)
            .map(|point| point.rate_percent().to_string())
            .as_deref(),
        Some("2.55")
    );

    let config = TreasurySourceConfig::daily_rates_all_families(2026, 2026)?;
    let doctor = config.doctor_plan(owner_use_attestation(Timestamp::from_unix_nanos(
        1_800_000_000_000_000_000,
    ))?)?;
    assert_eq!(doctor.probes().len(), TreasuryDailyRateFamily::ALL.len());
    Ok(())
}

#[test]
fn daily_rate_parser_accepts_nulls_and_terminal_pages_but_rejects_authority_drift() -> TestResult {
    let family = TreasuryDailyRateFamily::RealParYieldCurve;
    let month_request = TreasuryDailyRateQuery::month(family, 2026, 1)?.page(0)?;
    let fixture =
        std::str::from_utf8(include_bytes!("../fixtures/daily_real_par_yield_curve.xml"))?;
    let null_rate = fixture.replace(
        r#"<d:TC_30YEAR m:type="Edm.Double">2.63</d:TC_30YEAR>"#,
        r#"<d:TC_30YEAR m:type="Edm.Double" m:null="true" />"#,
    );
    let parsed = TreasuryDailyRatePage::parse(
        null_rate.as_bytes(),
        &month_request,
        FiscalDataParseLimits::production_defaults(),
    )?;
    assert!(
        parsed.observations()[0]
            .point(TreasuryDailyRateMetric::RealParYield(
                market_squawk_adapter_treasury::TreasuryMaturity::ThirtyYears,
            ))
            .is_none()
    );

    let wrong_month = TreasuryDailyRateQuery::month(family, 2026, 2)?.page(0)?;
    assert!(
        TreasuryDailyRatePage::parse(
            fixture.as_bytes(),
            &wrong_month,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );
    let unknown_field = fixture.replace(
        "</m:properties>",
        r#"<d:UNEXPECTED m:type="Edm.Double">1.0</d:UNEXPECTED></m:properties>"#,
    );
    assert!(
        TreasuryDailyRatePage::parse(
            unknown_field.as_bytes(),
            &month_request,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );
    let entry_start = fixture.find("<entry>").ok_or("entry start missing")?;
    let entry_end = fixture.find("</entry>").ok_or("entry end missing")? + "</entry>".len();
    let duplicated = fixture.replace(
        "</feed>",
        &format!("{}\n</feed>", &fixture[entry_start..entry_end]),
    );
    assert!(
        TreasuryDailyRatePage::parse(
            duplicated.as_bytes(),
            &month_request,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );

    let all = TreasuryDailyRateQuery::all_history(TreasuryDailyRateFamily::BillRates)?.page(9)?;
    let terminal = format!(
        r#"<?xml version="1.0"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>{}</title>
  <id>{}</id>
  <updated>2026-07-26T16:21:25Z</updated>
</feed>"#,
        TreasuryDailyRateFamily::BillRates.feed_title(),
        TreasuryDailyRateFamily::BillRates.feed_identity(),
    );
    assert!(
        TreasuryDailyRatePage::parse(
            terminal.as_bytes(),
            &all,
            FiscalDataParseLimits::production_defaults(),
        )?
        .is_terminal()
    );
    let malformed_terminal = terminal.replace("</feed>", "<entry /></feed>");
    assert!(
        TreasuryDailyRatePage::parse(
            malformed_terminal.as_bytes(),
            &all,
            FiscalDataParseLimits::production_defaults(),
        )
        .is_err()
    );

    let history = TreasuryDailyRateQuery::all_history(family)?;
    let first = TreasuryDailyRatePage::parse(
        fixture.as_bytes(),
        &history.page(0)?,
        FiscalDataParseLimits::production_defaults(),
    )?;
    let repeated = TreasuryDailyRatePage::parse(
        fixture.as_bytes(),
        &history.page(1)?,
        FiscalDataParseLimits::production_defaults(),
    )?;
    let mut tracker = TreasuryDailyRatePaginationTracker::try_new(&history, 4, 1_000)?;
    assert!(!tracker.accept(&first)?);
    assert!(tracker.accept(&repeated).is_err());
    Ok(())
}
