mod bulk {
    use std::error::Error;
    use std::io::{Cursor, Write as _};

    use cap_std::{ambient_authority, fs::Dir};
    use market_squawk_adapter_sec::{
        RawEvidenceStore, SecBulkCapture, SecBulkCoverage, SecBulkError, SecBulkFamily,
        SecBulkMediaKind, SecBulkNativePublicationSession, SecBulkParseLimits,
        SecBulkProjectionDisposition, SecBulkProviderProjection, SecBulkQueryLimits,
        SecBulkRelatedRowsState, SecBulkSelection, SecBulkTableKind, SecBulkTransportEvidence,
        SecBulkTypedValue, SecFundIdentityAuthority, SecFundPartitionAdmissions,
        SecFundPendingLogicalRows, SecFundPublicationScope, SecHttpValidators,
        SecPendingBulkLogicalPublication, SecQuarter, SecRepresentationLimits,
        SecRepresentationRegistry, inspect_bulk_archive, query_nport_holding_supplements,
        recover_bulk_archive, scan_bulk_archive, scan_bulk_archive_typed,
    };
    use market_squawk_domain::{
        EvidenceDigest, ExactPayloadEvidence, FundEvidenceRecord, FundHoldingSecurityIdentity,
        FundMissingState, FundShareClassIdentity, FundSourceTable, FundSupplementDisposition,
        InstrumentId, MetadataRevision, SourceId, SourceIdentifier, Timestamp,
    };
    use market_squawk_platform::{
        LocalPaths, ResearchObjectAdmission, ResearchObjectControl, ResearchObjectControlError,
        ResearchObjectControlPoint,
    };
    use market_squawk_sources::{LogicalPartitionFamily, LogicalPartitionSetAdmission};
    use tokio_util::sync::CancellationToken;
    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    struct AllowResearchObjects;

    impl ResearchObjectControl for AllowResearchObjects {
        fn checkpoint(
            &self,
            _point: ResearchObjectControlPoint,
        ) -> Result<(), ResearchObjectControlError> {
            Ok(())
        }
    }

    struct FixtureIdentityAuthority {
        observed_at: Timestamp,
        evidence: EvidenceDigest,
    }

    impl SecFundIdentityAuthority for FixtureIdentityAuthority {
        fn resolve_share_class(
            &mut self,
            series_id: &SourceIdentifier,
            _cutoff: Timestamp,
        ) -> Result<FundShareClassIdentity, SecBulkError> {
            FundShareClassIdentity::try_new(
                "4c7f46e9-58f0-49de-90ef-beb9d49a2884"
                    .parse::<InstrumentId>()
                    .map_err(|_| SecBulkError::UnresolvedIdentity)?,
                series_id.clone(),
                SourceId::try_from("fixture-reference")
                    .map_err(|_| SecBulkError::UnresolvedIdentity)?,
                MetadataRevision::new(SourceIdentifier::try_from("fixture-reference-v1")?),
                ExactPayloadEvidence::from_content_digest(self.evidence),
                self.observed_at,
                self.observed_at,
            )
            .map_err(|_| SecBulkError::UnresolvedIdentity)
        }

        fn resolve_holding_security(
            &mut self,
            _holding: &market_squawk_adapter_sec::SecNportHoldingRow,
            _identifiers: &[market_squawk_adapter_sec::SecNportIdentifierRow],
            _cutoff: Timestamp,
        ) -> Result<FundHoldingSecurityIdentity, SecBulkError> {
            FundHoldingSecurityIdentity::unresolved(FundMissingState::UnresolvedIdentity)
                .map_err(|_| SecBulkError::UnresolvedIdentity)
        }
    }

    #[test]
    fn quarterly_bulk_fund_topology_is_restart_safe_and_keeps_ncen_schema_gap_typed()
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
        let (archive_admission, readme_admission) =
            SecPendingBulkLogicalPublication::logical_object_admissions(&publication_manifest)?;
        let sealed = LocalPaths::prepare(temporary.path().join("sealed-journal"))?
            .sealed_research_journal_store()?;
        let control = AllowResearchObjects;
        let mut archive_stage = sealed.begin_logical_object(archive_admission)?;
        archive_stage.write_all(&archive_bytes)?;
        let archive_verified = sealed.finish_logical_object(archive_stage, &control)?;
        let mut readme_stage = sealed.begin_logical_object(readme_admission)?;
        readme_stage.write_all(readme_bytes)?;
        let readme_verified = sealed.finish_logical_object(readme_stage, &control)?;
        let pending = SecPendingBulkLogicalPublication::try_new(publication_manifest)?;
        let staged = pending.verify_and_stage(
            archive_verified,
            readme_verified,
            limits,
            deadline,
            &cancellation,
            &control,
            SecFundPendingLogicalRows::new(SecFundPublicationScope::try_ncen(
                SourceIdentifier::try_from("0001099263-26-004477")?,
                SourceIdentifier::try_from("0001099263-26-004477_0001795351_S000095886")?,
            )?),
        )?;
        let object_admission = ResearchObjectAdmission::try_new(8 * 1024 * 1024, 64)?;
        let partition_admission =
            LogicalPartitionSetAdmission::try_new(object_admission, 4, 100, 4 * 1024 * 1024)?;
        let observed_at = archive_received_at.max(readme_received_at);
        let mut identity = FixtureIdentityAuthority {
            observed_at,
            evidence: archive_evidence,
        };
        let prepared = staged.prepare_fund_logical_publication(
            &mut identity,
            observed_at,
            SecFundPartitionAdmissions::new(partition_admission, partition_admission),
            &sealed,
            &control,
        )?;
        assert_eq!(prepared.canonical_partitions().len(), 1);
        assert_eq!(prepared.canonical_partitions()[0].records().len(), 2);
        assert!(matches!(
            prepared.canonical_partitions()[0].records(),
            [
                FundEvidenceRecord::Report(_),
                FundEvidenceRecord::ShareClass(_)
            ]
        ));
        assert_eq!(prepared.terminal().total_canonical_rows, 2);
        assert_eq!(
            prepared
                .partitions()
                .iter()
                .map(|partition| partition.family())
                .collect::<Vec<_>>(),
            [
                LogicalPartitionFamily::ProviderNative,
                LogicalPartitionFamily::CanonicalRowMap
            ]
        );
        assert_ne!(
            prepared.preparation_digest(),
            prepared.canonical_partitions()[0].typed_input_digest()
        );

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
        prove_nport_holding_topology(temporary.path())?;
        Ok(())
    }

    fn prove_nport_holding_topology(root: &std::path::Path) -> Result<(), Box<dyn Error>> {
        let raw_path = root.join("nport-raw");
        let registry_path = root.join("nport-representations");
        std::fs::create_dir(&raw_path)?;
        std::fs::create_dir(&registry_path)?;
        let store = RawEvidenceStore::new(Dir::open_ambient_dir(&raw_path, ambient_authority())?);
        let registry = SecRepresentationRegistry::open(
            Dir::open_ambient_dir(&registry_path, ambient_authority())?,
            SecRepresentationLimits::production_defaults(),
        )?;
        let selection =
            SecBulkSelection::current(SecBulkFamily::Nport, SecQuarter::try_new(2026, 2)?)?;
        let archive_bytes = minimal_nport_archive()?;
        let archive_evidence = store.persist(&archive_bytes)?;
        let readme_bytes = b"official N-PORT readme evidence";
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
        let observed_at = archive_received_at.max(readme_received_at);
        let archive_capture = SecBulkCapture::try_from_registry_representation(
            selection.clone(),
            archive_representation,
            SecBulkTransportEvidence::try_new(
                200,
                SecBulkMediaKind::Zip,
                Some("application/zip"),
                SecHttpValidators::default(),
                archive_received_at,
            )?,
        )?;
        let readme_capture = SecBulkCapture::try_from_registry_representation(
            selection,
            readme_representation,
            SecBulkTransportEvidence::try_new(
                200,
                SecBulkMediaKind::Pdf,
                Some("application/pdf"),
                SecHttpValidators::default(),
                readme_received_at,
            )?,
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
        )?;

        let mut native = SecBulkNativePublicationSession::new(
            &store,
            manifest.clone(),
            observed_at,
            deadline,
            cancellation.clone(),
        )?;
        scan_bulk_archive(
            &store,
            &manifest,
            limits,
            deadline,
            &cancellation,
            &mut native,
        )?;
        let native = native
            .published_generation()
            .cloned()
            .ok_or(SecBulkError::PublicationNotReady)?;
        let supplements = query_nport_holding_supplements(
            &store,
            &native,
            &SourceIdentifier::try_from("0000000001-26-000001")?,
            &SourceIdentifier::try_from("101")?,
            SecBulkQueryLimits::try_new(1_000, 100)?,
            deadline,
            &cancellation,
        )?;
        assert_eq!(supplements.tables().len(), 19);
        for table in [
            SecBulkTableKind::NportIdentifiers,
            SecBulkTableKind::NportDebtSecurity,
        ] {
            let related = supplements
                .tables()
                .iter()
                .find(|related| related.table() == table)
                .ok_or(SecBulkError::InvalidCanonicalMapping)?;
            assert_eq!(related.state(), SecBulkRelatedRowsState::ReportedRows);
            assert_eq!(related.rows().len(), 1);
            assert_eq!(related.rows()[0].primary_key()[0].value(), "101");
        }

        let (archive_admission, readme_admission) =
            SecPendingBulkLogicalPublication::logical_object_admissions(&manifest)?;
        let sealed = LocalPaths::prepare(root.join("nport-sealed-journal"))?
            .sealed_research_journal_store()?;
        let control = AllowResearchObjects;
        let mut archive_stage = sealed.begin_logical_object(archive_admission)?;
        archive_stage.write_all(&archive_bytes)?;
        let archive_verified = sealed.finish_logical_object(archive_stage, &control)?;
        let mut readme_stage = sealed.begin_logical_object(readme_admission)?;
        readme_stage.write_all(readme_bytes)?;
        let readme_verified = sealed.finish_logical_object(readme_stage, &control)?;
        let staged = SecPendingBulkLogicalPublication::try_new(manifest)?.verify_and_stage(
            archive_verified,
            readme_verified,
            limits,
            deadline,
            &cancellation,
            &control,
            SecFundPendingLogicalRows::new(SecFundPublicationScope::try_nport(
                SourceIdentifier::try_from("0000000001-26-000001")?,
            )?),
        )?;
        let partition_admission = LogicalPartitionSetAdmission::try_new(
            ResearchObjectAdmission::try_new(8 * 1024 * 1024, 64)?,
            4,
            100,
            4 * 1024 * 1024,
        )?;
        let prepared = staged.prepare_fund_logical_publication(
            &mut FixtureIdentityAuthority {
                observed_at,
                evidence: archive_evidence,
            },
            observed_at,
            SecFundPartitionAdmissions::new(partition_admission, partition_admission),
            &sealed,
            &control,
        )?;
        let records = prepared
            .canonical_partitions()
            .iter()
            .flat_map(|partition| partition.records())
            .collect::<Vec<_>>();
        assert_eq!(records.len(), 3);
        let holding = records
            .iter()
            .find_map(|record| match record {
                FundEvidenceRecord::PortfolioHolding(holding) => Some(holding.as_ref()),
                _ => None,
            })
            .ok_or(SecBulkError::InvalidCanonicalMapping)?;
        assert_eq!(holding.holding_id().as_str(), "101");
        for table in [
            FundSourceTable::NportIdentifiers,
            FundSourceTable::NportDebtSecurity,
        ] {
            assert!(
                holding
                    .lineage()
                    .rows()
                    .iter()
                    .any(|row| row.table() == table)
            );
        }
        assert!(
            !holding
                .lineage()
                .rows()
                .iter()
                .any(|row| { row.table() == FundSourceTable::NportExplanatoryNote })
        );
        assert_eq!(
            holding
                .supplements()
                .iter()
                .find(|supplement| supplement.table() == FundSourceTable::NportDebtSecurity)
                .ok_or(SecBulkError::InvalidCanonicalMapping)?
                .disposition(),
            FundSupplementDisposition::Reported
        );
        let report = records
            .iter()
            .find_map(|record| match record {
                FundEvidenceRecord::Report(report) => Some(report.as_ref()),
                _ => None,
            })
            .ok_or(SecBulkError::InvalidCanonicalMapping)?;
        assert_eq!(
            report
                .lineage()
                .rows()
                .iter()
                .filter(|row| row.table() == FundSourceTable::NportExplanatoryNote)
                .count(),
            1
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

    fn minimal_nport_archive() -> Result<Vec<u8>, Box<dyn Error>> {
        const SUBMISSION_HEADER: &str = "ACCESSION_NUMBER\tFILING_DATE\tSUB_TYPE\tREPORT_ENDING_PERIOD\tREPORT_DATE\tIS_LAST_FILING";
        const REGISTRANT_HEADER: &str = "ACCESSION_NUMBER\tCIK\tREGISTRANT_NAME\tLEI";
        const FUND_HEADER: &str = "ACCESSION_NUMBER\tSERIES_NAME\tSERIES_ID\tSERIES_LEI\tTOTAL_ASSETS\tTOTAL_LIABILITIES\tNET_ASSETS";
        const HOLDING_HEADER: &str = "ACCESSION_NUMBER\tHOLDING_ID\tISSUER_NAME\tISSUER_LEI\tISSUER_TITLE\tISSUER_CUSIP\tBALANCE\tUNIT\tOTHER_UNIT_DESC\tCURRENCY_CODE\tCURRENCY_VALUE\tEXCHANGE_RATE\tPERCENTAGE\tPAYOFF_PROFILE\tASSET_CAT\tOTHER_ASSET\tISSUER_TYPE\tOTHER_ISSUER\tINVESTMENT_COUNTRY\tIS_RESTRICTED_SECURITY\tFAIR_VALUE_LEVEL\tDERIVATIVE_CAT";
        const IDENTIFIER_HEADER: &str = "HOLDING_ID\tIDENTIFIERS_ID\tIDENTIFIER_ISIN\tIDENTIFIER_TICKER\tOTHER_IDENTIFIER\tOTHER_IDENTIFIER_DESC";
        const RELATED_HEADER: &str = "HOLDING_ID\tDETAIL";
        const EXPLANATORY_NOTE_HEADER: &str =
            "ACCESSION_NUMBER\tEXPLANATORY_NOTE_ID\tITEM_NO\tEXPLANATORY_NOTE";
        let tables = NPORT_TABLES
            .iter()
            .map(|name| match *name {
                "SUBMISSION.tsv" => metadata_table_for(
                    "nport_readme.htm",
                    name,
                    SUBMISSION_HEADER,
                    &["ACCESSION_NUMBER"],
                ),
                "REGISTRANT.tsv" => metadata_table_for(
                    "nport_readme.htm",
                    name,
                    REGISTRANT_HEADER,
                    &["ACCESSION_NUMBER"],
                ),
                "FUND_REPORTED_INFO.tsv" => {
                    metadata_table_for("nport_readme.htm", name, FUND_HEADER, &["ACCESSION_NUMBER"])
                }
                "FUND_REPORTED_HOLDING.tsv" => {
                    metadata_table_for("nport_readme.htm", name, HOLDING_HEADER, &["HOLDING_ID"])
                }
                "IDENTIFIERS.tsv" => metadata_table_for(
                    "nport_readme.htm",
                    name,
                    IDENTIFIER_HEADER,
                    &["HOLDING_ID", "IDENTIFIERS_ID"],
                ),
                "EXPLANATORY_NOTE.tsv" => metadata_table_for(
                    "nport_readme.htm",
                    name,
                    EXPLANATORY_NOTE_HEADER,
                    &["ACCESSION_NUMBER", "EXPLANATORY_NOTE_ID"],
                ),
                name if NPORT_HOLDING_SUPPLEMENTS.contains(&name) => {
                    metadata_table_for("nport_readme.htm", name, RELATED_HEADER, &["HOLDING_ID"])
                }
                _ => metadata_table_for("nport_readme.htm", name, "ROW_ID", &["ROW_ID"]),
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
            "nport_metadata.json",
            metadata.as_bytes(),
            options,
        )?;
        write_member(&mut writer, "nport_readme.htm", b"readme", options)?;
        write_member(
            &mut writer,
            "SUBMISSION.tsv",
            format!(
                "{SUBMISSION_HEADER}\n0000000001-26-000001\t15-MAY-2026\tNPORT-P\t31-DEC-2025\t31-MAR-2026\tN\n0000000002-26-000002\t15-MAY-2026\tNPORT-P\t31-DEC-2025\t31-MAR-2026\tN\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "REGISTRANT.tsv",
            format!(
                "{REGISTRANT_HEADER}\n0000000001-26-000001\t0000000001\tSelected Fund\t54930000000000000001\n0000000002-26-000002\t0000000002\tOther Fund\t54930000000000000002\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "FUND_REPORTED_INFO.tsv",
            format!(
                "{FUND_HEADER}\n0000000001-26-000001\tSelected Fund\tS000000001\t54930000000000000011\t1000000\t10000\t990000\n0000000002-26-000002\tOther Fund\tS000000002\t54930000000000000012\t2000000\t20000\t1980000\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "FUND_REPORTED_HOLDING.tsv",
            format!(
                "{HOLDING_HEADER}\n0000000001-26-000001\t101\tApple Inc.\t54930000000000000101\tCommon Stock\t037833100\t10\tshares\t\tUSD\t1000\t1\t5\tLONG\tEC\t\tCORP\t\tUS\tN\t1\t\n0000000002-26-000002\t202\tOther Issuer\t54930000000000000202\tCommon Stock\t594918104\t20\tshares\t\tUSD\t2000\t1\t10\tLONG\tEC\t\tCORP\t\tUS\tN\t1\t\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "IDENTIFIERS.tsv",
            format!(
                "{IDENTIFIER_HEADER}\n101\t1\tUS0378331005\tAAPL\t\t\n202\t2\tUS5949181045\tMSFT\t\t\n"
            )
            .as_bytes(),
            options,
        )?;
        write_member(
            &mut writer,
            "EXPLANATORY_NOTE.tsv",
            format!(
                "{EXPLANATORY_NOTE_HEADER}\n0000000001-26-000001\t1\tB.1\tselected filing note\n0000000002-26-000002\t2\tB.1\tother filing note\n"
            )
            .as_bytes(),
            options,
        )?;
        for name in NPORT_TABLES.iter().copied().filter(|name| {
            !matches!(
                *name,
                "SUBMISSION.tsv"
                    | "REGISTRANT.tsv"
                    | "FUND_REPORTED_INFO.tsv"
                    | "FUND_REPORTED_HOLDING.tsv"
                    | "IDENTIFIERS.tsv"
                    | "EXPLANATORY_NOTE.tsv"
            )
        }) {
            let contents = if name == "DEBT_SECURITY.tsv" {
                format!("{RELATED_HEADER}\n101\tselected evidence\n202\tother evidence\n")
            } else if NPORT_HOLDING_SUPPLEMENTS.contains(&name) {
                format!("{RELATED_HEADER}\n")
            } else {
                "ROW_ID\n".to_owned()
            };
            write_member(&mut writer, name, contents.as_bytes(), options)?;
        }
        Ok(writer.finish()?.into_inner())
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

    const NPORT_HOLDING_SUPPLEMENTS: [&str; 18] = [
        "DEBT_SECURITY.tsv",
        "DEBT_SECURITY_REF_INSTRUMENT.tsv",
        "CONVERTIBLE_SECURITY_CURRENCY.tsv",
        "REPURCHASE_AGREEMENT.tsv",
        "REPURCHASE_COUNTERPARTY.tsv",
        "REPURCHASE_COLLATERAL.tsv",
        "DERIVATIVE_COUNTERPARTY.tsv",
        "SWAPTION_OPTION_WARNT_DERIV.tsv",
        "DESC_REF_INDEX_BASKET.tsv",
        "DESC_REF_INDEX_COMPONENT.tsv",
        "DESC_REF_OTHER.tsv",
        "FUT_FWD_NONFOREIGNCUR_CONTRACT.tsv",
        "FWD_FOREIGNCUR_CONTRACT_SWAP.tsv",
        "NONFOREIGN_EXCHANGE_SWAP.tsv",
        "FLOATING_RATE_RESET_TENOR.tsv",
        "OTHER_DERIV.tsv",
        "OTHER_DERIV_NOTIONAL_AMOUNT.tsv",
        "SECURITIES_LENDING.tsv",
    ];

    const NPORT_TABLES: [&str; 30] = [
        "SUBMISSION.tsv",
        "REGISTRANT.tsv",
        "FUND_REPORTED_INFO.tsv",
        "INTEREST_RATE_RISK.tsv",
        "BORROWER.tsv",
        "BORROW_AGGREGATE.tsv",
        "MONTHLY_TOTAL_RETURN.tsv",
        "MONTHLY_RETURN_CAT_INSTRUMENT.tsv",
        "FUND_VAR_INFO.tsv",
        "FUND_REPORTED_HOLDING.tsv",
        "IDENTIFIERS.tsv",
        "DEBT_SECURITY.tsv",
        "DEBT_SECURITY_REF_INSTRUMENT.tsv",
        "CONVERTIBLE_SECURITY_CURRENCY.tsv",
        "REPURCHASE_AGREEMENT.tsv",
        "REPURCHASE_COUNTERPARTY.tsv",
        "REPURCHASE_COLLATERAL.tsv",
        "DERIVATIVE_COUNTERPARTY.tsv",
        "SWAPTION_OPTION_WARNT_DERIV.tsv",
        "DESC_REF_INDEX_BASKET.tsv",
        "DESC_REF_INDEX_COMPONENT.tsv",
        "DESC_REF_OTHER.tsv",
        "FUT_FWD_NONFOREIGNCUR_CONTRACT.tsv",
        "FWD_FOREIGNCUR_CONTRACT_SWAP.tsv",
        "NONFOREIGN_EXCHANGE_SWAP.tsv",
        "FLOATING_RATE_RESET_TENOR.tsv",
        "OTHER_DERIV.tsv",
        "OTHER_DERIV_NOTIONAL_AMOUNT.tsv",
        "SECURITIES_LENDING.tsv",
        "EXPLANATORY_NOTE.tsv",
    ];

    fn metadata_table(name: &str, header: &str, primary_key: &[&str]) -> String {
        metadata_table_for("ncen_readme.htm", name, header, primary_key)
    }

    fn metadata_table_for(readme: &str, name: &str, header: &str, primary_key: &[&str]) -> String {
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
            r#"{{"url":"{name}","tableSchema":{{"aboutUrl":"{readme}","PrimaryKey":[{keys}],"columns":[{columns}]}}}}"#
        )
    }

    fn metadata_column(column: &str) -> String {
        let datatype = match column {
            "FILING_DATE" | "REPORT_ENDING_PERIOD" | "REPORT_DATE" => {
                r#"{"base":"date (DD-MON-YYYY)"}"#.to_owned()
            }
            "TOTAL_SERIES" | "HOLDING_ID" | "IDENTIFIERS_ID" | "EXPLANATORY_NOTE_ID" => {
                r#"{"base":"NUMBER","dataPrecision":38,"dataScale":0,"maxLength":22}"#.to_owned()
            }
            "MONTHLY_AVG_NET_ASSETS"
            | "DAILY_AVG_NET_ASSETS"
            | "NUM_SHARES_PER_CREATION_UNIT"
            | "REDEEMED_SHARES_PER_CREATION_UNIT"
            | "TOTAL_ASSETS"
            | "TOTAL_LIABILITIES"
            | "NET_ASSETS"
            | "BALANCE"
            | "CURRENCY_VALUE"
            | "EXCHANGE_RATE"
            | "PERCENTAGE" => {
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
            "ITEM_NO" => 50,
            "EXPLANATORY_NOTE" => 4_000,
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
