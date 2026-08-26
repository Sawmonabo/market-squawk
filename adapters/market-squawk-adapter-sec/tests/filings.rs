mod bulk {
    use std::error::Error;
    use std::io::{Cursor, Write as _};

    use cap_std::{ambient_authority, fs::Dir};
    use market_squawk_adapter_sec::{
        RawEvidenceStore, SecBulkCapture, SecBulkCoverage, SecBulkError, SecBulkFamily,
        SecBulkKeyField, SecBulkMediaKind, SecBulkNativePublicationSession, SecBulkParseLimits,
        SecBulkProjectionDisposition, SecBulkProviderProjection, SecBulkQueryLimits,
        SecBulkSelection, SecBulkTableKind, SecBulkTransportEvidence, SecBulkTypedValue,
        SecHttpValidators, SecQuarter, SecRepresentationLimits, SecRepresentationRegistry,
        inspect_bulk_archive, query_native_rows, recover_bulk_archive,
        recover_native_generation_from_receipt, scan_bulk_archive, scan_bulk_archive_typed,
    };
    use market_squawk_domain::Timestamp;
    use tokio_util::sync::CancellationToken;
    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    #[test]
    fn quarterly_bulk_is_exact_restart_safe_and_keeps_ncen_schema_gap_typed()
    -> Result<(), Box<dyn Error>> {
        let temporary = tempfile::tempdir()?;
        let raw_path = temporary.path().join("raw");
        let registry_path = temporary.path().join("representations");
        std::fs::create_dir(&raw_path)?;
        std::fs::create_dir(&registry_path)?;
        let store = RawEvidenceStore::new(Dir::open_ambient_dir(&raw_path, ambient_authority())?);
        let registry = SecRepresentationRegistry::open(
            Dir::open_ambient_dir(&registry_path, ambient_authority())?,
            SecRepresentationLimits::production_defaults(),
        )?;
        let selection =
            SecBulkSelection::current(SecBulkFamily::Ncen, SecQuarter::try_new(2026, 2)?)?;
        assert!(matches!(
            selection.coverage(),
            SecBulkCoverage::AcceptedSchemaExcluded { schema }
                if schema.version().as_str() == "3.1"
        ));
        assert!(!selection.coverage().missing_row_proves_no_filing());

        let archive_bytes = minimal_ncen_archive()?;
        let archive_evidence = store.persist(&archive_bytes)?;
        let readme_bytes = b"official N-CEN readme evidence";
        let readme_evidence = store.persist(readme_bytes)?;
        let archive_representation = registry.record_success(
            selection.archive_locator().as_str(),
            archive_evidence,
            u64::try_from(archive_bytes.len())?,
            SecHttpValidators::default(),
        )?;
        let readme_representation = registry.record_success(
            selection.readme_locator().as_str(),
            readme_evidence,
            u64::try_from(readme_bytes.len())?,
            SecHttpValidators::default(),
        )?;
        let archive_received_at = archive_representation.first_observed_at();
        let readme_received_at = readme_representation.first_observed_at();
        let archive_transport = SecBulkTransportEvidence::try_new(
            200,
            SecBulkMediaKind::Zip,
            Some("application/zip"),
            SecHttpValidators::default(),
            archive_received_at,
        )?;
        let readme_transport = SecBulkTransportEvidence::try_new(
            200,
            SecBulkMediaKind::Pdf,
            Some("application/pdf"),
            SecHttpValidators::default(),
            readme_received_at,
        )?;
        let archive_capture = SecBulkCapture::try_from_registry_representation(
            selection.clone(),
            archive_representation,
            archive_transport,
        )?;
        let readme_capture = SecBulkCapture::try_from_registry_representation(
            selection.clone(),
            readme_representation,
            readme_transport,
        )?;
        let cancellation = CancellationToken::new();
        let limits = SecBulkParseLimits::production_defaults();
        let deadline = Timestamp::from_unix_nanos(i64::MAX);
        let manifest = inspect_bulk_archive(
            &store,
            archive_capture,
            readme_capture,
            limits,
            deadline,
            &cancellation,
        )
        .map_err(|error| std::io::Error::other(format!("inspect bulk archive: {error:?}")))?;
        assert_eq!(manifest.tables().len(), 45);
        assert_eq!(manifest.absent_declared_tables().len(), 8);
        assert!(
            manifest
                .table("SEC_LENDING_IDEMNITY_PROVIDER.tsv")
                .is_some()
        );
        let invalid_registry_path = temporary.path().join("invalid-representations");
        std::fs::create_dir(&invalid_registry_path)?;
        let invalid_registry = SecRepresentationRegistry::open(
            Dir::open_ambient_dir(&invalid_registry_path, ambient_authority())?,
            SecRepresentationLimits::production_defaults(),
        )?;
        let invalid_archive_bytes = with_declared_eocd_entries(archive_bytes.clone(), 65)?;
        let invalid_archive_evidence = store.persist(&invalid_archive_bytes)?;
        let invalid_archive_representation = invalid_registry.record_success(
            selection.archive_locator().as_str(),
            invalid_archive_evidence,
            u64::try_from(invalid_archive_bytes.len())?,
            SecHttpValidators::default(),
        )?;
        let invalid_readme_representation = invalid_registry.record_success(
            selection.readme_locator().as_str(),
            readme_evidence,
            u64::try_from(readme_bytes.len())?,
            SecHttpValidators::default(),
        )?;
        let invalid_archive_received_at = invalid_archive_representation.first_observed_at();
        let invalid_readme_received_at = invalid_readme_representation.first_observed_at();
        let invalid_archive_capture = SecBulkCapture::try_from_registry_representation(
            selection.clone(),
            invalid_archive_representation,
            SecBulkTransportEvidence::try_new(
                200,
                SecBulkMediaKind::Zip,
                Some("application/zip"),
                SecHttpValidators::default(),
                invalid_archive_received_at,
            )?,
        )?;
        let invalid_readme_capture = SecBulkCapture::try_from_registry_representation(
            selection.clone(),
            invalid_readme_representation,
            SecBulkTransportEvidence::try_new(
                200,
                SecBulkMediaKind::Pdf,
                Some("application/pdf"),
                SecHttpValidators::default(),
                invalid_readme_received_at,
            )?,
        )?;
        assert!(matches!(
            inspect_bulk_archive(
                &store,
                invalid_archive_capture,
                invalid_readme_capture,
                limits,
                deadline,
                &cancellation,
            ),
            Err(SecBulkError::EntryLimitExceeded)
        ));

        let publication_manifest =
            recover_bulk_archive(&store, &manifest, limits, deadline, &cancellation)?;
        let published_at = archive_received_at.max(readme_received_at);
        let mut publication = SecBulkNativePublicationSession::new(
            &store,
            publication_manifest,
            published_at,
            deadline,
            cancellation.clone(),
        )?;
        scan_bulk_archive(
            &store,
            &manifest,
            limits,
            deadline,
            &cancellation,
            &mut publication,
        )
        .map_err(|error| std::io::Error::other(format!("publish native bulk: {error:?}")))?;
        let generation_receipt = publication
            .published_generation()
            .ok_or_else(|| std::io::Error::other("native generation was not published"))?
            .receipt();
        drop(publication);
        let generation = recover_native_generation_from_receipt(
            &store,
            generation_receipt,
            deadline,
            &cancellation,
        )
        .map_err(|error| std::io::Error::other(format!("recover native receipt: {error:?}")))?;
        assert_eq!(generation.receipt(), generation_receipt);
        let primary_key = [SecBulkKeyField::try_new(
            "ACCESSION_NUMBER",
            "0001099263-26-004477",
        )?];
        let page = query_native_rows(
            &store,
            &generation,
            SecBulkTableKind::NcenSubmission,
            Some(&primary_key),
            SecBulkQueryLimits::production_defaults(),
            None,
            deadline,
            &cancellation,
        )
        .map_err(|error| std::io::Error::other(format!("query native row: {error:?}")))?;
        assert_eq!(page.rows().len(), 1);
        assert!(page.next_cursor().is_none());
        assert!(matches!(
            page.rows()[0].projection_disposition(),
            SecBulkProjectionDisposition::Projected(
                SecBulkProviderProjection::NcenSubmission(candidate)
            ) if candidate.accession.as_str() == "0001099263-26-004477"
        ));

        let typed_manifest =
            recover_bulk_archive(&store, &manifest, limits, deadline, &cancellation)?;
        let mut dispositions = Vec::new();
        let scan = scan_bulk_archive_typed(
            &store,
            typed_manifest,
            limits,
            deadline,
            &cancellation,
            |row| {
                dispositions.push(match row.projection_disposition() {
                    SecBulkProjectionDisposition::Projected(
                        SecBulkProviderProjection::NcenSubmission(candidate),
                    ) if candidate.accession.as_str() == "0001099263-26-004477" => "submission",
                    SecBulkProjectionDisposition::Projected(
                        SecBulkProviderProjection::NcenRegistrant(candidate),
                    ) if candidate.cik.as_str() == "0001795351" => "registrant",
                    SecBulkProjectionDisposition::Projected(
                        SecBulkProviderProjection::NcenFund(candidate),
                    ) if candidate
                        .series_id
                        .as_ref()
                        .is_some_and(|series| series.as_str() == "S000095886")
                        && candidate.is_etf == Some(true) =>
                    {
                        "fund"
                    }
                    SecBulkProjectionDisposition::UnresolvedIdentity
                        if row.table() == SecBulkTableKind::NcenSecurityExchange =>
                    {
                        assert!(row.fields().iter().any(|field| {
                            field.name().as_str() == "FUND_EXCHANGE"
                                && matches!(
                                    field.value(),
                                    SecBulkTypedValue::Text(value) if value == "NYSE ARCA"
                                )
                        }));
                        assert!(row.fields().iter().any(|field| {
                            field.name().as_str() == "FUND_TICKER_SYMBOL"
                                && matches!(
                                    field.value(),
                                    SecBulkTypedValue::Text(value) if value == "PRIH"
                                )
                        }));
                        assert!(row.row_evidence().bytes().iter().any(|byte| *byte != 0));
                        "exchange-unresolved"
                    }
                    SecBulkProjectionDisposition::Projected(
                        SecBulkProviderProjection::NcenEtf(candidate),
                    ) if candidate
                        .series_id
                        .as_ref()
                        .is_some_and(|series| series.as_str() == "S000095886")
                        && candidate.collateral_required == Some(false)
                        && candidate.is_in_kind_etf == Some(true) =>
                    {
                        "etf"
                    }
                    SecBulkProjectionDisposition::NotApplicable => "not-applicable",
                    _ => return Err(SecBulkError::InvalidLayout),
                });
                Ok(())
            },
        )
        .map_err(|error| std::io::Error::other(format!("consume typed scan: {error:?}")))?;
        assert_eq!(
            dispositions,
            [
                "submission",
                "registrant",
                "not-applicable",
                "fund",
                "exchange-unresolved",
                "etf"
            ]
        );
        assert_eq!(scan.manifest().evidence(), manifest.evidence());
        assert_eq!(scan.report().source_rows(), 6);
        assert_eq!(scan.report().emitted_typed_rows(), 6);
        assert_eq!(
            scan.archive_capture().first_observed_at(),
            archive_received_at
        );
        assert_eq!(
            scan.official_readme_capture().first_observed_at(),
            readme_received_at
        );
        assert_ne!(
            scan.archive_capture().evidence(),
            scan.official_readme_capture().evidence()
        );
        Ok(())
    }

    fn with_declared_eocd_entries(
        mut archive: Vec<u8>,
        entries: u16,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let eocd = archive
            .windows(4)
            .rposition(|window| window == [0x50, 0x4b, 0x05, 0x06])
            .ok_or_else(|| std::io::Error::other("fixture ZIP has no EOCD"))?;
        archive[eocd + 8..eocd + 10].copy_from_slice(&entries.to_le_bytes());
        archive[eocd + 10..eocd + 12].copy_from_slice(&entries.to_le_bytes());
        Ok(archive)
    }

    fn minimal_ncen_archive() -> Result<Vec<u8>, Box<dyn Error>> {
        const SUBMISSION_HEADER: &str = "ACCESSION_NUMBER\tSUBMISSION_TYPE\tCIK\tFILING_DATE\tREPORT_ENDING_PERIOD\tIS_REPORT_PERIOD_LT_12MONTH";
        const REGISTRANT_HEADER: &str = "ACCESSION_NUMBER\tREGISTRANT_NAME\tFILE_NUM\tCIK\tLEI\tINVESTMENT_COMPANY_TYPE\tTOTAL_SERIES";
        const FUND_HEADER: &str = "FUND_ID\tACCESSION_NUMBER\tFUND_NAME\tSERIES_ID\tLEI\tIS_ETF\tIS_INDEX\tMONTHLY_AVG_NET_ASSETS\tDAILY_AVG_NET_ASSETS";
        const ETF_HEADER: &str = "FUND_ID\tFUND_NAME\tSERIES_ID\tIS_COLLATERAL_REQUIRED\tNUM_SHARES_PER_CREATION_UNIT\tREDEEMED_SHARES_PER_CREATION_UNIT\tIS_FUND_IN_KIND_ETF";
        const SECURITY_EXCHANGE_HEADER: &str = "FUND_ID\tFUND_EXCHANGE\tFUND_TICKER_SYMBOL";
        let tables = NCEN_TABLES
            .iter()
            .map(|name| match *name {
                "SUBMISSION.tsv" => metadata_table(name, SUBMISSION_HEADER, &["ACCESSION_NUMBER"]),
                "REGISTRANT.tsv" => metadata_table(name, REGISTRANT_HEADER, &["ACCESSION_NUMBER"]),
                "FUND_REPORTED_INFO.tsv" => {
                    metadata_table(name, FUND_HEADER, &["ACCESSION_NUMBER", "SERIES_ID"])
                }
                "ETF.tsv" => metadata_table(name, ETF_HEADER, &["FUND_ID"]),
                "SECURITY_EXCHANGE.tsv" => {
                    metadata_table(name, SECURITY_EXCHANGE_HEADER, &["FUND_ID"])
                }
                _ => metadata_table(name, "ROW_ID", &[]),
            })
            .collect::<Vec<_>>()
            .join(",");
        let metadata = format!(
            r#"{{"@context":"http://www.w3.org/ns/csvw","dialect":{{"header":true,"headerRowCount":1,"delimiter":"\t"}},"tables":[{tables}]}}"#,
        );
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        write_member(
            &mut writer,
            "ncen_metadata.json",
            metadata.as_bytes(),
            options,
        )?;
        write_member(&mut writer, "ncen_readme.htm", b"readme", options)?;
        write_member(
            &mut writer,
            "SUBMISSION.tsv",
            format!(
                "{SUBMISSION_HEADER}\n0001099263-26-004477\tN-CEN\t0001795351\t12-MAY-2026\t28-FEB-2026\tN\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "REGISTRANT.tsv",
            format!(
                "{REGISTRANT_HEADER}\n0001099263-26-004477\tT. Rowe Price Exchange-Traded Funds, Inc.\t811-23494\t0001795351\t549300XZPUK24E1UMH17\tN-1A\t31\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "FUND_REPORTED_INFO.tsv",
            format!(
                "{FUND_HEADER}\n0001099263-26-004477_0001795351_S000095886\t0001099263-26-004477\tT. Rowe Price High Income Municipal ETF\tS000095886\t254900IAXRMS6C2LLA91\tY\tN\t22629616\t22610000\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "SECURITY_EXCHANGE.tsv",
            format!(
                "{SECURITY_EXCHANGE_HEADER}\n0001099263-26-004477_0001795351_S000095886\tNYSE ARCA\tPRIH\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "ETF.tsv",
            format!(
                "{ETF_HEADER}\n0001099263-26-004477_0001795351_S000095886\tT. Rowe Price High Income Municipal ETF\tS000095886\tN\t50000\t50000\tY\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "REGISTRANT_WEBSITE.tsv",
            b"ROW_ID\nrow-1\n",
            options,
        )?;
        for name in NCEN_TABLES.iter().copied().filter(|name| {
            !matches!(
                *name,
                "SUBMISSION.tsv"
                    | "REGISTRANT.tsv"
                    | "REGISTRANT_WEBSITE.tsv"
                    | "FUND_REPORTED_INFO.tsv"
                    | "SECURITY_EXCHANGE.tsv"
                    | "ETF.tsv"
            ) && !NCEN_DECLARED_ABSENT.contains(name)
        }) {
            let header = match name {
                "ETF.tsv" => ETF_HEADER,
                "SECURITY_EXCHANGE.tsv" => SECURITY_EXCHANGE_HEADER,
                _ => "ROW_ID",
            };
            write_member(&mut writer, name, format!("{header}\n").as_bytes(), options)?;
        }
        Ok(writer.finish()?.into_inner())
    }

    const NCEN_DECLARED_ABSENT: [&str; 8] = [
        "DIVESTMENT.tsv",
        "DIVIDENDS_IN_ARREAR.tsv",
        "LONGTERM_DEBT_DEFAULT.tsv",
        "REGISTRANT_HELDS_SECURITY.tsv",
        "SERIES_CIK.tsv",
        "SPONSOR.tsv",
        "TRUSTEE.tsv",
        "VALUATION_METHOD_CHANGE_SERIES.tsv",
    ];

    const NCEN_TABLES: [&str; 53] = [
        "SUBMISSION.tsv",
        "REGISTRANT.tsv",
        "REGISTRANT_WEBSITE.tsv",
        "LOCATION_BOOKS_RECORD.tsv",
        "TERMINATED_ORGANIZATION.tsv",
        "DIRECTOR.tsv",
        "DIRECTOR_FILE_NUMBER.tsv",
        "CHIEF_COMPLIANCE_OFFICER.tsv",
        "CCO_EMPLOYER.tsv",
        "REGISTRANT_REPORTING_SERIES.tsv",
        "RELEASE_NUMBER.tsv",
        "PRINCIPAL_UNDERWRITER.tsv",
        "PUBLIC_ACCOUNTANT.tsv",
        "VALUATION_METHOD_CHANGE.tsv",
        "VALUATION_METHOD_CHANGE_SERIES.tsv",
        "FUND_REPORTED_INFO.tsv",
        "SHARES_OUTSTANDING.tsv",
        "FEEDER_FUNDS.tsv",
        "MASTER_FUNDS.tsv",
        "FOREIGN_INVESTMENT.tsv",
        "SECURITY_LENDING.tsv",
        "SEC_LENDING_IDEMNITY_PROVIDER.tsv",
        "COLLATERAL_MANAGER.tsv",
        "ADVISER.tsv",
        "TRANSFER_AGENT.tsv",
        "PRICING_SERVICE.tsv",
        "CUSTODIAN.tsv",
        "SHAREHOLDER_SERVICING_AGENT.tsv",
        "ADMIN.tsv",
        "BROKER_DEALER.tsv",
        "BROKER.tsv",
        "PRINCIPAL_TRANSACTION.tsv",
        "LINE_OF_CREDIT_DETAIL.tsv",
        "LINE_OF_CREDIT_INSTITUTION.tsv",
        "CREDIT_USER.tsv",
        "INTER_FUND_LENDING_DETAIL.tsv",
        "INTER_FUND_BORROWING_DETAIL.tsv",
        "SECURITY_RELATED_ITEM.tsv",
        "RIGHTS_OFFERING_FUND.tsv",
        "LONGTERM_DEBT_DEFAULT.tsv",
        "DIVIDENDS_IN_ARREAR.tsv",
        "SECURITY_EXCHANGE.tsv",
        "AUTHORIZED_PARTICIPANT.tsv",
        "ETF.tsv",
        "DEPOSITOR.tsv",
        "UIT_ADMIN.tsv",
        "UIT.tsv",
        "SERIES_CIK.tsv",
        "SPONSOR.tsv",
        "TRUSTEE.tsv",
        "CONTRACT_SECURITY.tsv",
        "DIVESTMENT.tsv",
        "REGISTRANT_HELDS_SECURITY.tsv",
    ];

    fn metadata_table(name: &str, header: &str, primary_key: &[&str]) -> String {
        let columns = header
            .split('\t')
            .map(metadata_column)
            .collect::<Vec<_>>()
            .join(",");
        let keys = primary_key
            .iter()
            .map(|key| format!(r#""{key}""#))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            r#"{{"url":"{name}","tableSchema":{{"aboutUrl":"ncen_readme.htm","PrimaryKey":[{keys}],"columns":[{columns}]}}}}"#
        )
    }

    fn metadata_column(column: &str) -> String {
        let datatype = match column {
            "FILING_DATE" | "REPORT_ENDING_PERIOD" => r#"{"base":"date (DD-MON-YYYY)"}"#.to_owned(),
            "TOTAL_SERIES" => {
                r#"{"base":"NUMBER","dataPrecision":8,"dataScale":0,"maxLength":22}"#.to_owned()
            }
            "MONTHLY_AVG_NET_ASSETS"
            | "DAILY_AVG_NET_ASSETS"
            | "NUM_SHARES_PER_CREATION_UNIT"
            | "REDEEMED_SHARES_PER_CREATION_UNIT" => {
                r#"{"base":"NUMBER","dataPrecision":"NULL","dataScale":"NULL","maxLength":22}"#
                    .to_owned()
            }
            _ => format!(
                r#"{{"base":"string","maxLength":{}}}"#,
                string_length(column)
            ),
        };
        format!(
            r#"{{"name":"{column}","titles":"{column}","datatype":{datatype},"dc:description":"test contract for {column}"}}"#
        )
    }

    fn string_length(column: &str) -> u64 {
        match column {
            "ACCESSION_NUMBER" => 20,
            "SUBMISSION_TYPE" | "CIK" | "INVESTMENT_COMPANY_TYPE" | "SERIES_ID" => 10,
            "IS_REPORT_PERIOD_LT_12MONTH"
            | "IS_ETF"
            | "IS_INDEX"
            | "IS_COLLATERAL_REQUIRED"
            | "IS_FUND_IN_KIND_ETF" => 1,
            "REGISTRANT_NAME" | "FUND_NAME" => 150,
            "FILE_NUM" => 30,
            "LEI" => 20,
            "FUND_ID" => 42,
            _ => 128,
        }
    }

    fn write_member(
        writer: &mut ZipWriter<Cursor<Vec<u8>>>,
        name: &str,
        bytes: &[u8],
        options: SimpleFileOptions,
    ) -> Result<(), Box<dyn Error>> {
        writer.start_file(name, options)?;
        writer.write_all(bytes)?;
        Ok(())
    }
}
#[path = "evidence_store.rs"]
mod evidence_store;
#[path = "locators.rs"]
mod locators;
#[path = "official_fixtures.rs"]
mod official_fixtures;
#[path = "restart_reconcile.rs"]
mod restart_reconcile;
