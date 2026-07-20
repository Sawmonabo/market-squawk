use super::*;

#[tokio::test]
async fn xlsx_rejects_archive_traversal_and_case_collisions() -> Result<(), Box<dyn Error>> {
    let directory = tempfile::tempdir()?;
    let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
    archive.start_file("../outside.xml", SimpleFileOptions::default())?;
    archive.write_all(b"<outside/>")?;
    let payload = archive.finish()?.into_inner();
    fs::write(directory.path().join("hostile.xlsx"), payload)?;

    let manifest = manifest("hostile.xlsx", "excel");
    fs::write(directory.path().join("manifest.json"), &manifest)?;
    let root = UserAuthorizedInputRoot::open(fs::canonicalize(directory.path())?)?;
    let manifest_input = root
        .resolve("manifest.json")?
        .open_bounded(16 * 1024)?
        .read_bounded()?;
    let source = FileExtractionSource::try_new_with_clock(
        local_metadata(&manifest)?,
        root,
        manifest_input,
        ExtractionLimits::try_new(ExtractionLimitsInput::standard())?,
        fixed_clock(),
    )?;
    let discovery = DiscoveryRequest::try_new(
        SourceIdentifier::try_from("alternative-prices")?,
        None,
        NonZeroU16::new(1).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    let discovered = source
        .discover_files(&discovery, &CancellationToken::new())
        .await?;
    let request = ExtractionRequest::try_new(
        discovered.objects()[0].clone(),
        NonZeroU32::new(16).ok_or("nonzero")?,
        NonZeroU64::new(1024 * 1024).ok_or("nonzero")?,
        Timestamp::from_unix_nanos(10_000_000_000),
    )?;
    let error = source
        .extract_file(&request, &CancellationToken::new())
        .await
        .err()
        .ok_or("hostile XLSX unexpectedly succeeded")?;
    assert_eq!(error, FileAdapterError::UnsafeArchive);

    let mut archive = ZipWriter::new(Cursor::new(Vec::new()));
    for name in ["xl/workbook.xml", "XL/WORKBOOK.XML"] {
        archive.start_file(name, SimpleFileOptions::default())?;
        archive.write_all(b"<workbook/>")?;
    }
    fs::write(
        directory.path().join("collision.xlsx"),
        archive.finish()?.into_inner(),
    )?;
    let error = extract_fixture(&directory, "collision.xlsx", "excel")
        .await?
        .err()
        .ok_or("case-colliding XLSX unexpectedly succeeded")?;
    assert_eq!(error, FileAdapterError::UnsafeArchive);

    let oversized_entries = with_eocd_entry_count(xlsx_package(b"<worksheet/>")?, 60_000)?;
    fs::write(
        directory.path().join("oversized-entries.xlsx"),
        oversized_entries,
    )?;
    let error = extract_fixture(&directory, "oversized-entries.xlsx", "excel")
        .await?
        .err()
        .ok_or("oversized ZIP entry declaration unexpectedly succeeded")?;
    assert_eq!(
        error,
        FileAdapterError::LimitExceeded(ParserLimit::ArchiveEntries)
    );

    let large_value = "7".repeat(64 * 1024);
    let large_sheet = format!(
        r#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>id</t></is></c><c r="B1" t="inlineStr"><is><t>value</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>one</t></is></c><c r="B2" t="inlineStr"><is><t>{large_value}</t></is></c></row></sheetData></worksheet>"#
    );
    let retained_payload = xlsx_package(large_sheet.as_bytes())?;
    fs::write(
        directory.path().join("retained-before-decompress.xlsx"),
        &retained_payload,
    )?;
    let mut limits = ExtractionLimitsInput::standard();
    limits.max_source_bytes = u64::try_from(retained_payload.len())?;
    limits.max_decompressed_bytes = limits.max_source_bytes.max(256 * 1024);
    limits.max_retained_bytes = 32 * 1024;
    limits.max_compression_ratio = 10_000;
    let error = extract_fixture_with_limits(
        &directory,
        "retained-before-decompress.xlsx",
        "excel",
        limits,
    )
    .await?
    .err()
    .ok_or("XLSX declared payload escaped the retained allocation limit")?;
    assert_eq!(
        error,
        FileAdapterError::LimitExceeded(ParserLimit::DecodedBytes)
    );
    Ok(())
}

#[tokio::test]
async fn xlsx_extracts_flat_rows_and_rejects_formulas_by_policy() -> Result<(), Box<dyn Error>> {
    let valid_sheet = br#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>id</t></is></c><c r="B1" t="inlineStr"><is><t>value</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>one</t></is></c><c r="B2" t="inlineStr"><is><t>12.50</t></is></c></row></sheetData></worksheet>"#;
    let content_types = br#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Types>"#;
    let root_relationships = br#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Relationships>"#;
    let workbook = br#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Data" sheetId="1" r:id="rId1"/></sheets></workbook>"#;
    let workbook_relationships = br#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/></Relationships>"#;
    let directory = tempfile::tempdir()?;
    fs::write(
        directory.path().join("valid.xlsx"),
        xlsx_package(valid_sheet)?,
    )?;
    let batch = extract_fixture(&directory, "valid.xlsx", "excel").await??;
    assert_eq!(batch.records().len(), 1);
    let observation: ResearchObservation = serde_json::from_slice(batch.records()[0].payload())?;
    let ResearchObservation::AlternativeData(observation) = observation else {
        return Err("XLSX produced the wrong canonical observation kind".into());
    };
    assert!(matches!(
        observation.context().provenance().payload_reference(),
        PayloadReference::SourceReference(reference)
            if reference.as_str().starts_with("local-object-row:canonical-sha256:")
    ));

    let duplicate_shared_relationships = br#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/><Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/></Relationships>"#;
    let content_types_with_shared = br#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/xl/sharedStrings.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml"/></Types>"#;
    let shared_strings = br#"<sst xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><si><t>unused</t></si></sst>"#;
    let shared_sheet = br#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1" t="inlineStr"><is><t>value</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>one</t></is></c><c r="B2" t="inlineStr"><is><t>12.50</t></is></c></row></sheetData></worksheet>"#;
    let one_shared_relationship = br#"<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/><Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings" Target="sharedStrings.xml"/></Relationships>"#;
    let invalid_packages = vec![
        (
            "missing-root-relationship.xlsx",
            xlsx_custom_package(
                valid_sheet,
                content_types,
                None,
                workbook,
                workbook_relationships,
                None,
            )?,
        ),
        (
            "wrong-content-type.xlsx",
            xlsx_custom_package(
                valid_sheet,
                br#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Override PartName="/xl/workbook.xml" ContentType="application/xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/xml"/></Types>"#,
                Some(root_relationships),
                workbook,
                workbook_relationships,
                None,
            )?,
        ),
        (
            "duplicate-shared-strings.xlsx",
            xlsx_custom_package(
                valid_sheet,
                content_types_with_shared,
                Some(root_relationships),
                workbook,
                duplicate_shared_relationships,
                Some(shared_strings),
            )?,
        ),
        (
            "wrong-content-types-root.xlsx",
            xlsx_custom_package(
                valid_sheet,
                br#"<Catalog xmlns="urn:not-opc"><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/></Catalog>"#,
                Some(root_relationships),
                workbook,
                workbook_relationships,
                None,
            )?,
        ),
        (
            "wrong-relationships-root.xlsx",
            xlsx_custom_package(
                valid_sheet,
                content_types,
                Some(br#"<Catalog xmlns="urn:not-opc"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/></Catalog>"#),
                workbook,
                workbook_relationships,
                None,
            )?,
        ),
        (
            "wrong-workbook-root.xlsx",
            xlsx_custom_package(
                valid_sheet,
                content_types,
                Some(root_relationships),
                br#"<catalog xmlns="urn:not-spreadsheet" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets><sheet name="Data" sheetId="1" r:id="rId1"/></sheets></catalog>"#,
                workbook_relationships,
                None,
            )?,
        ),
        (
            "wrong-worksheet-root.xlsx",
            xlsx_custom_package(
                br#"<catalog xmlns="urn:not-spreadsheet"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>id</t></is></c><c r="B1" t="inlineStr"><is><t>value</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>one</t></is></c><c r="B2" t="inlineStr"><is><t>12.50</t></is></c></row></sheetData></catalog>"#,
                content_types,
                Some(root_relationships),
                workbook,
                workbook_relationships,
                None,
            )?,
        ),
        (
            "wrong-shared-strings-root.xlsx",
            xlsx_custom_package(
                shared_sheet,
                content_types_with_shared,
                Some(root_relationships),
                workbook,
                one_shared_relationship,
                Some(br#"<catalog xmlns="urn:not-spreadsheet"><si><t>id</t></si></catalog>"#),
            )?,
        ),
        (
            "nested-duplicate-opc-graph.xlsx",
            xlsx_custom_package(
                br#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>id</t></is></c><c r="B1" t="inlineStr"><is><t>value</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>one</t></is></c><c r="B2" t="inlineStr"><is><t>12.50</t></is></c></row></sheetData></sheetData></worksheet>"#,
                br#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types"><Default Extension="xml" ContentType="application/xml"><Default Extension="xml" ContentType="application/xml"/></Default><Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/><Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/><Override PartName="/absent.xml" ContentType="application/xml"/></Types>"#,
                Some(root_relationships),
                br#"<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><container><sheet name="Data" sheetId="1" r:id="rId1"/></container></workbook>"#,
                workbook_relationships,
                None,
            )?,
        ),
    ];
    for (name, package) in invalid_packages {
        fs::write(directory.path().join(name), package)?;
        let error = extract_fixture(&directory, name, "excel")
            .await?
            .err()
            .ok_or("invalid OPC package unexpectedly succeeded")?;
        assert_eq!(error, FileAdapterError::UnsafeSpreadsheet, "{name}");
    }

    let formula_sheet = br#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData><row r="1"><c r="A1" t="inlineStr"><is><t>id</t></is></c><c r="B1" t="inlineStr"><is><t>value</t></is></c></row><row r="2"><c r="A2" t="inlineStr"><is><t>one</t></is></c><c r="B2"><f>10+2.5</f><v>12.5</v></c></row></sheetData></worksheet>"#;
    fs::write(
        directory.path().join("formula.xlsx"),
        xlsx_package(formula_sheet)?,
    )?;
    let error = extract_fixture(&directory, "formula.xlsx", "excel")
        .await?
        .err()
        .ok_or("formula workbook unexpectedly succeeded")?;
    assert_eq!(error, FileAdapterError::UnsafeSpreadsheet);

    fs::write(
        directory.path().join("escape.xlsx"),
        xlsx_package_with_relationship(valid_sheet, "../../../outside.xml")?,
    )?;
    let error = extract_fixture(&directory, "escape.xlsx", "excel")
        .await?
        .err()
        .ok_or("escaping OOXML relationship unexpectedly succeeded")?;
    assert_eq!(error, FileAdapterError::UnsafeSpreadsheet);
    Ok(())
}
